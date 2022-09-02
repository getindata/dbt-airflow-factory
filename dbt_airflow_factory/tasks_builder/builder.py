"""Class parsing ``manifest.json`` into Airflow tasks."""
import json
import logging
from typing import Any, ContextManager, Dict, Tuple

import airflow
from airflow.models.baseoperator import BaseOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from dbt_airflow_factory.tasks_builder.node_type import NodeType
from dbt_airflow_factory.tasks_builder.parameters import TasksBuildingParameters

if not airflow.__version__.startswith("1."):
    from airflow.utils.task_group import TaskGroup

from dbt_airflow_factory.operator import DbtRunOperatorBuilder, EphemeralOperator
from dbt_airflow_factory.tasks import ModelExecutionTask, ModelExecutionTasks
from dbt_airflow_factory.tasks_builder.gateway import TaskGraphConfiguration
from dbt_airflow_factory.tasks_builder.graph import DbtAirflowGraph


class DbtAirflowTasksBuilder:
    """
    Parses ``manifest.json`` into Airflow tasks.

    :param airflow_config: DBT node operator.
    :type airflow_config: TasksBuildingParameters
    :param operator_builder: DBT node operator.
    :type operator_builder: DbtRunOperatorBuilder
    :param gateway_config: DBT node operator.
    :type gateway_config: TaskGraphConfiguration
    """

    def __init__(
        self,
        airflow_config: TasksBuildingParameters,
        operator_builder: DbtRunOperatorBuilder,
        gateway_config: TaskGraphConfiguration,
    ):
        self.operator_builder = operator_builder
        self.airflow_config = airflow_config
        self.gateway_config = gateway_config

    def parse_manifest_into_tasks(self, manifest_path: str) -> ModelExecutionTasks:
        """
        Parse ``manifest.json`` into tasks.

        :param manifest_path: Path to ``manifest.json``.
        :type manifest_path: str
        :return: Dictionary of tasks created from ``manifest.json`` parsing.
        :rtype: ModelExecutionTasks
        """
        return self._make_dbt_tasks(manifest_path)

    def create_seed_task(self) -> BaseOperator:
        """
        Create ``dbt_seed`` task.

        :return: Operator for ``dbt_seed`` task.
        :rtype: BaseOperator
        """
        return self.operator_builder.create("dbt_seed", "seed")

    @staticmethod
    def _load_dbt_manifest(manifest_path: str) -> dict:
        with open(manifest_path, "r") as f:
            manifest_content = json.load(f)
            logging.debug("Manifest content: " + str(manifest_content))
            return manifest_content

    def _make_dbt_test_task(self, model_name: str, is_in_task_group: bool) -> BaseOperator:
        command = "test"
        return self.operator_builder.create(
            self._build_task_name(model_name, command, is_in_task_group),
            command,
            model_name,
            additional_dbt_args=["--indirect-selection=cautious"],
        )

    def _make_dbt_multiple_deps_test_task(
        self, test_names: str, dependency_tuple_str: str
    ) -> BaseOperator:
        command = "test"
        return self.operator_builder.create(dependency_tuple_str, command, test_names)

    def _make_dbt_run_task(self, model_name: str, is_in_task_group: bool) -> BaseOperator:
        command = "run"
        return self.operator_builder.create(
            self._build_task_name(model_name, command, is_in_task_group),
            command,
            model_name,
        )

    @staticmethod
    def _build_task_name(model_name: str, command: str, is_in_task_group: bool) -> str:
        return command if is_in_task_group else f"{model_name}_{command}"

    @staticmethod
    def _create_task_group_for_model(
        model_name: str, use_task_group: bool
    ) -> Tuple[Any, ContextManager]:
        import contextlib

        is_first_version = airflow.__version__.startswith("1.")
        task_group = (
            None if (is_first_version or not use_task_group) else TaskGroup(group_id=model_name)
        )
        task_group_ctx = task_group or contextlib.nullcontext()
        return task_group, task_group_ctx

    def _create_task_for_model(
        self,
        model_name: str,
        is_ephemeral_task: bool,
        use_task_group: bool,
    ) -> ModelExecutionTask:
        if is_ephemeral_task:
            return ModelExecutionTask(EphemeralOperator(task_id=f"{model_name}__ephemeral"), None)

        (task_group, task_group_ctx) = self._create_task_group_for_model(model_name, use_task_group)
        is_in_task_group = task_group is not None
        with task_group_ctx:
            run_task = self._make_dbt_run_task(model_name, is_in_task_group)
            test_task = self._make_dbt_test_task(model_name, is_in_task_group)
            # noinspection PyStatementEffect
            run_task >> test_task
        return ModelExecutionTask(run_task, test_task, task_group)

    def _create_task_from_graph_node(
        self, node_name: str, node: Dict[str, Any]
    ) -> ModelExecutionTask:
        if node["node_type"] == NodeType.MULTIPLE_DEPS_TEST:
            return ModelExecutionTask(
                self._make_dbt_multiple_deps_test_task(node["select"], node_name), None
            )
        elif node["node_type"] == NodeType.SOURCE_SENSOR:
            return self._create_dag_sensor(node)
        elif node["node_type"] == NodeType.MOCK_GATEWAY:
            return self._create_dummy_task(node)
        else:
            return self._create_task_for_model(
                node["select"],
                node["node_type"] == NodeType.EPHEMERAL,
                self.airflow_config.use_task_group,
            )

    def _create_tasks_from_graph(self, dbt_airflow_graph: DbtAirflowGraph) -> ModelExecutionTasks:
        result_tasks = {
            node_name: self._create_task_from_graph_node(node_name, node)
            for node_name, node in dbt_airflow_graph.graph.nodes(data=True)
        }
        for node, neighbour in dbt_airflow_graph.graph.edges():
            # noinspection PyStatementEffect
            (result_tasks[node].get_end_task() >> result_tasks[neighbour].get_start_task())
        return ModelExecutionTasks(
            result_tasks,
            dbt_airflow_graph.get_graph_sources(),
            dbt_airflow_graph.get_graph_sinks(),
        )

    def _make_dbt_tasks(self, manifest_path: str) -> ModelExecutionTasks:
        manifest = self._load_dbt_manifest(manifest_path)
        dbt_airflow_graph = self._create_tasks_graph(manifest)
        tasks_with_context = self._create_tasks_from_graph(dbt_airflow_graph)
        logging.info(f"Created {str(tasks_with_context.length())} tasks groups")
        return tasks_with_context

    def _create_tasks_graph(self, manifest: dict) -> DbtAirflowGraph:

        dbt_airflow_graph = DbtAirflowGraph(self.gateway_config)
        dbt_airflow_graph.add_execution_tasks(manifest)
        if self.airflow_config.enable_dags_dependencies:
            dbt_airflow_graph.add_external_dependencies(manifest)
        dbt_airflow_graph.create_edges_from_dependencies(
            self.airflow_config.enable_dags_dependencies
        )
        if not self.airflow_config.show_ephemeral_models:
            dbt_airflow_graph.remove_ephemeral_nodes_from_graph()
        dbt_airflow_graph.contract_test_nodes()
        return dbt_airflow_graph

    def _create_dag_sensor(self, node: Dict[str, Any]) -> ModelExecutionTask:
        # todo move parameters to configuration
        return ModelExecutionTask(
            ExternalTaskSensor(
                task_id="sensor_" + node["select"],
                external_dag_id=node["dag"],
                external_task_id=node["select"]
                + (".test" if self.airflow_config.use_task_group else "_test"),
                timeout=24 * 60 * 60,
                allowed_states=["success"],
                failed_states=["failed", "skipped"],
                mode="reschedule",
            )
        )

    @staticmethod
    def _create_dummy_task(node: Dict[str, Any]) -> ModelExecutionTask:
        return ModelExecutionTask(DummyOperator(task_id=node["select"]))
