"""Class parsing ``manifest.json`` into Airflow tasks."""
import itertools
import json
import logging
from enum import Enum
from typing import Any, ContextManager, Dict, List, Tuple

import airflow
from airflow.models.baseoperator import BaseOperator

if not airflow.__version__.startswith("1."):
    from airflow.utils.task_group import TaskGroup

import networkx as nx

from dbt_airflow_factory.operator import DbtRunOperatorBuilder, EphemeralOperator
from dbt_airflow_factory.tasks import ModelExecutionTask, ModelExecutionTasks


class NodeType(Enum):
    RUN_TEST = 1
    MULTIPLE_DEPS_TEST = 2
    EPHEMERAL = 3


class DbtAirflowGraph:
    graph: nx.DiGraph

    def __init__(self) -> None:
        self.graph = nx.DiGraph()

    def parse_manifest_into_graph(self, manifest: dict) -> None:
        self._create_nodes_from_manifest(manifest["nodes"])
        self._create_edges_from_dependencies()

    def get_graph_sources(self) -> List[str]:
        return [
            node_name
            for node_name in self.graph.nodes()
            if len(list(self.graph.predecessors(node_name))) == 0
        ]

    def get_graph_sinks(self) -> List[str]:
        return [
            node_name
            for node_name in self.graph.nodes()
            if len(list(self.graph.successors(node_name))) == 0
        ]

    def remove_ephemeral_nodes_from_graph(self) -> None:
        ephemeral_nodes = [
            node_name
            for node_name, node in self.graph.nodes(data=True)
            if node["node_type"] == NodeType.EPHEMERAL
        ]
        for node_name in ephemeral_nodes:
            self.graph.add_edges_from(
                itertools.product(
                    list(self.graph.predecessors(node_name)), list(self.graph.successors(node_name))
                )
            )
            self.graph.remove_node(node_name)

    def contract_test_nodes(self) -> None:
        tests_with_more_deps = self._get_test_with_multiple_deps_names_by_deps()
        for depends_on_tuple, test_node_names in tests_with_more_deps.items():
            self._contract_test_nodes_same_deps(depends_on_tuple, test_node_names)

    def _add_graph_node(
        self, node_name: str, manifest_node: Dict[str, Any], node_type: NodeType
    ) -> None:
        self.graph.add_node(
            node_name,
            select=manifest_node["name"],
            depends_on=self._get_model_dependencies_from_manifest_node(manifest_node),
            node_type=node_type,
        )

    def _add_graph_node_for_model_run_task(
        self, node_name: str, manifest_node: Dict[str, Any]
    ) -> None:
        self._add_graph_node(
            node_name,
            manifest_node,
            NodeType.EPHEMERAL
            if DbtAirflowTasksBuilder.is_ephemeral_task(manifest_node)
            else NodeType.RUN_TEST,
        )

    def _add_graph_node_for_multiple_deps_test(
        self, node_name: str, manifest_node: Dict[str, Any]
    ) -> None:
        self._add_graph_node(node_name, manifest_node, NodeType.MULTIPLE_DEPS_TEST)

    def _create_nodes_from_manifest(self, manifest_nodes: Dict[str, Any]) -> None:
        for node_name, manifest_node in manifest_nodes.items():
            if DbtAirflowTasksBuilder.is_model_run_task(node_name):
                logging.info("Creating tasks for: " + node_name)
                self._add_graph_node_for_model_run_task(node_name, manifest_node)
            elif (
                DbtAirflowTasksBuilder.is_test_task(node_name)
                and len(self._get_model_dependencies_from_manifest_node(manifest_node)) > 1
            ):
                logging.info("Creating tasks for: " + node_name)
                self._add_graph_node_for_multiple_deps_test(node_name, manifest_node)

    def _create_edges_from_dependencies(self) -> None:
        for graph_node_name, graph_node in self.graph.nodes(data=True):
            for dependency in graph_node["depends_on"]:
                self.graph.add_edge(dependency, graph_node_name)

    def _get_test_with_multiple_deps_names_by_deps(self) -> Dict[Tuple[str, ...], List[str]]:
        tests_with_more_deps: Dict[Tuple[str, ...], List[str]] = {}

        for node_name, node in self.graph.nodes(data=True):
            if node["node_type"] == NodeType.MULTIPLE_DEPS_TEST:
                model_dependencies = node["depends_on"]
                model_dependencies.sort()
                if tuple(model_dependencies) not in tests_with_more_deps:
                    tests_with_more_deps[tuple(model_dependencies)] = []
                tests_with_more_deps[tuple(model_dependencies)].append(node_name)

        return tests_with_more_deps

    def _contract_test_nodes_same_deps(
        self, depends_on_tuple: Tuple[str, ...], test_node_names: List[str]
    ) -> None:
        test_names = [self.graph.nodes[test_node]["select"] for test_node in test_node_names]

        first_test_node = test_node_names[0]
        for test_node in test_node_names[1:]:
            nx.contracted_nodes(
                self.graph, first_test_node, test_node, self_loops=False, copy=False  # in-memory
            )

        self.graph.nodes[first_test_node]["select"] = " ".join(test_names)
        nx.relabel_nodes(
            self.graph,
            {first_test_node: self._build_multiple_deps_test_name(depends_on_tuple)},
            copy=False,
        )

    @staticmethod
    def _get_model_dependencies_from_manifest_node(node: Dict[str, Any]) -> List[str]:
        return list(filter(DbtAirflowTasksBuilder.is_model_run_task, node["depends_on"]["nodes"]))

    @staticmethod
    def _build_multiple_deps_test_name(dependencies: tuple) -> str:
        return "_".join(map(lambda node_name: node_name.split(".")[-1], dependencies)) + "_test"


class DbtAirflowTasksBuilder:
    """
    Parses ``manifest.json`` into Airflow tasks.

    :param operator_builder: DBT node operator.
    :type operator_builder: DbtRunOperatorBuilder
    """

    def __init__(self, operator_builder: DbtRunOperatorBuilder):
        self.operator_builder = operator_builder

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
    def _is_task_type(node_name: str, task_type: str) -> bool:
        return node_name.split(".")[0] == task_type

    @staticmethod
    def is_model_run_task(node_name: str) -> bool:
        return DbtAirflowTasksBuilder._is_task_type(node_name, "model")

    @staticmethod
    def is_test_task(node_name: str) -> bool:
        return DbtAirflowTasksBuilder._is_task_type(node_name, "test")

    @staticmethod
    def is_ephemeral_task(node: dict) -> bool:
        return node["config"]["materialized"] == "ephemeral"

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
        self, node_name: str, node: Dict[str, Any], use_task_group: bool
    ) -> ModelExecutionTask:
        return (
            ModelExecutionTask(
                self._make_dbt_multiple_deps_test_task(node["select"], node_name), None
            )
            if node["node_type"] == NodeType.MULTIPLE_DEPS_TEST
            else self._create_task_for_model(
                node["select"], node["node_type"] == NodeType.EPHEMERAL, use_task_group
            )
        )

    def _create_tasks_from_graph(
        self, dbt_airflow_graph: DbtAirflowGraph, use_task_group: bool
    ) -> ModelExecutionTasks:
        result_tasks = {
            node_name: self._create_task_from_graph_node(node_name, node, use_task_group)
            for node_name, node in dbt_airflow_graph.graph.nodes(data=True)
        }
        for node, neighbour in dbt_airflow_graph.graph.edges():
            # noinspection PyStatementEffect
            result_tasks[node].get_end_task() >> result_tasks[neighbour].get_start_task()

        return ModelExecutionTasks(
            result_tasks, dbt_airflow_graph.get_graph_sources(), dbt_airflow_graph.get_graph_sinks()
        )

    def _make_dbt_tasks(
        self, manifest_path: str, use_task_group: bool, show_ephemeral_models: bool
    ) -> ModelExecutionTasks:
        manifest = self._load_dbt_manifest(manifest_path)

        dbt_airflow_graph = DbtAirflowGraph()
        dbt_airflow_graph.parse_manifest_into_graph(manifest)
        if not show_ephemeral_models:
            dbt_airflow_graph.remove_ephemeral_nodes_from_graph()
        dbt_airflow_graph.contract_test_nodes()

        tasks_with_context = self._create_tasks_from_graph(dbt_airflow_graph, use_task_group)
        logging.info(f"Created {str(tasks_with_context.length())} tasks groups")
        return tasks_with_context

    def parse_manifest_into_tasks(
        self,
        manifest_path: str,
        use_task_group: bool = True,
        show_ephemeral_models: bool = True,
    ) -> ModelExecutionTasks:
        """
        Parse ``manifest.json`` into tasks.

        :param manifest_path: Path to ``manifest.json``.
        :type manifest_path: str
        :param use_task_group: Whether to use TaskGroup (does not work in Airflow 1).
        :type use_task_group: bool
        :param show_ephemeral_models: Whether to show ephemeral models in Airflow DAG.
        :type show_ephemeral_models: bool
        :return: Dictionary of tasks created from ``manifest.json`` parsing.
        :rtype: ModelExecutionTasks
        """
        return self._make_dbt_tasks(manifest_path, use_task_group, show_ephemeral_models)

    def create_seed_task(self) -> BaseOperator:
        """
        Create ``dbt_seed`` task.

        :return: Operator for ``dbt_seed`` task.
        :rtype: BaseOperator
        """
        return self.operator_builder.create("dbt_seed", "seed")
