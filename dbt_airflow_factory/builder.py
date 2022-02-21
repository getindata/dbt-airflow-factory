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
        self, test_names: List[str], dependencies: tuple
    ) -> BaseOperator:
        command = "test"
        return self.operator_builder.create(
            self._build_multiple_deps_test_name(dependencies),
            command,
            " ".join(test_names),
        )

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
    def _build_multiple_deps_test_name(dependencies: tuple) -> str:
        return "_".join(map(lambda node_name: node_name.split(".")[-1], dependencies)) + "_test"

    @staticmethod
    def _is_task_type(node_name: str, task_type: str) -> bool:
        return node_name.split(".")[0] == task_type

    @staticmethod
    def _is_model_run_task(node_name: str) -> bool:
        return DbtAirflowTasksBuilder._is_task_type(node_name, "model")

    @staticmethod
    def _is_test_task(node_name: str) -> bool:
        return DbtAirflowTasksBuilder._is_task_type(node_name, "test")

    @staticmethod
    def _is_ephemeral_task(node: dict) -> bool:
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

    def _parse_manifest_into_graph(self, manifest: dict) -> nx.DiGraph:
        res_graph = nx.DiGraph()

        for node_name, node in [
            (str(k), v) for k, v in manifest["nodes"].items() if self._is_model_run_task(k)
        ]:
            logging.info("Creating tasks for: " + node_name)
            res_graph.add_node(
                node_name,
                name=node["name"],
                depends_on=node["depends_on"]["nodes"],
                node_type=NodeType.EPHEMERAL
                if self._is_ephemeral_task(node)
                else NodeType.RUN_TEST,
            )

        for node_name, node in [
            (str(k), v) for k, v in manifest["nodes"].items() if self._is_test_task(k)
        ]:
            model_dependencies = list(filter(self._is_model_run_task, node["depends_on"]["nodes"]))
            if len(model_dependencies) > 1:
                logging.info("Creating tasks for: " + node_name)
                res_graph.add_node(
                    node_name,
                    name=node["name"],
                    depends_on=model_dependencies,
                    node_type=NodeType.MULTIPLE_DEPS_TEST,
                )

        for node_name, node in res_graph.nodes(data=True):
            for dependency in filter(lambda dep: self._is_model_run_task(dep), node["depends_on"]):
                res_graph.add_edge(dependency, node_name)

        return res_graph

    @staticmethod
    def _remove_ephemeral_nodes_from_graph(graph: nx.DiGraph) -> None:
        ephemeral_nodes = [
            node_name
            for node_name, node in graph.nodes(data=True)
            if node["node_type"] == NodeType.EPHEMERAL
        ]
        for node_name in ephemeral_nodes:
            graph.add_edges_from(
                itertools.product(
                    list(graph.predecessors(node_name)), list(graph.successors(node_name))
                )
            )
            graph.remove_node(node_name)

    def _create_tasks_from_graph(
        self, graph: nx.DiGraph, use_task_group: bool
    ) -> ModelExecutionTasks:
        starting_tasks = set()
        ending_tasks = set()
        result_tasks = {}
        tests_with_more_deps: Dict[Tuple[str, ...], List[str]] = {}

        def get_node_name(node_name: str) -> str:
            if graph.nodes[node_name]["node_type"] == NodeType.MULTIPLE_DEPS_TEST:
                model_dependencies = graph.nodes[node_name]["depends_on"]
                model_dependencies.sort()
                return self._build_multiple_deps_test_name(tuple(model_dependencies))
            return node_name

        for node_name, node in graph.nodes(data=True):
            if len(list(graph.predecessors(node_name))) == 0:
                starting_tasks.add(get_node_name(node_name))
            if len(list(graph.successors(node_name))) == 0:
                ending_tasks.add(get_node_name(node_name))

        for node_name, node in graph.nodes(data=True):
            if node["node_type"] == NodeType.MULTIPLE_DEPS_TEST:
                model_dependencies = node["depends_on"]
                model_dependencies.sort()
                if tuple(model_dependencies) not in tests_with_more_deps:
                    tests_with_more_deps[tuple(model_dependencies)] = []
                tests_with_more_deps[tuple(model_dependencies)].append(node["name"])
            else:  # NodeType.RUN_TEST, NodeType.EPHEMERAL
                result_tasks[node_name] = self._create_task_for_model(
                    node["name"], node["node_type"] == NodeType.EPHEMERAL, use_task_group
                )

        result_tasks = dict(
            result_tasks,
            **{
                self._build_multiple_deps_test_name(depends_on_tuple): ModelExecutionTask(
                    self._make_dbt_multiple_deps_test_task(test_names, depends_on_tuple), None
                )
                for depends_on_tuple, test_names in tests_with_more_deps.items()
            },
        )

        for node, neighbour in graph.edges():
            # noinspection PyStatementEffect
            (
                result_tasks[get_node_name(node)].get_end_task()
                >> result_tasks[get_node_name(neighbour)].get_start_task()
            )

        return ModelExecutionTasks(result_tasks, list(starting_tasks), list(ending_tasks))

    def _make_dbt_tasks(
        self, manifest_path: str, use_task_group: bool, show_ephemeral_models: bool
    ) -> ModelExecutionTasks:
        manifest = self._load_dbt_manifest(manifest_path)
        graph = self._parse_manifest_into_graph(manifest)
        if not show_ephemeral_models:
            self._remove_ephemeral_nodes_from_graph(graph)
        tasks_with_context = self._create_tasks_from_graph(graph, use_task_group)
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
