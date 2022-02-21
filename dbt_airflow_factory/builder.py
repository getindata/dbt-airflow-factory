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

    def _get_model_dependencies_from_manifest_node(self, node: Dict[str, Any]) -> List[str]:
        return list(filter(self._is_model_run_task, node["depends_on"]["nodes"]))

    def _add_graph_node(
        self, graph: nx.DiGraph, node_name: str, node: Dict[str, Any], node_type: NodeType
    ) -> None:
        graph.add_node(
            node_name,
            select=node["name"],
            depends_on=self._get_model_dependencies_from_manifest_node(node),
            node_type=node_type,
        )

    def _parse_manifest_into_graph(self, manifest: dict) -> nx.DiGraph:
        res_graph = nx.DiGraph()

        for node_name, manifest_node in manifest["nodes"].items():
            if self._is_model_run_task(node_name):
                logging.info("Creating tasks for: " + node_name)
                self._add_graph_node(
                    res_graph,
                    node_name,
                    manifest_node,
                    NodeType.EPHEMERAL
                    if self._is_ephemeral_task(manifest_node)
                    else NodeType.RUN_TEST,
                )
            elif (
                self._is_test_task(node_name)
                and len(self._get_model_dependencies_from_manifest_node(manifest_node)) > 1
            ):
                logging.info("Creating tasks for: " + node_name)
                self._add_graph_node(
                    res_graph, node_name, manifest_node, NodeType.MULTIPLE_DEPS_TEST
                )

        for graph_node_name, graph_node in res_graph.nodes(data=True):
            for dependency in graph_node["depends_on"]:
                res_graph.add_edge(dependency, graph_node_name)

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

    def _contract_test_nodes(self, graph: nx.DiGraph) -> None:
        tests_with_more_deps: Dict[Tuple[str, ...], List[str]] = {}

        for node_name, node in graph.nodes(data=True):
            if node["node_type"] == NodeType.MULTIPLE_DEPS_TEST:
                model_dependencies = node["depends_on"]
                model_dependencies.sort()
                if tuple(model_dependencies) not in tests_with_more_deps:
                    tests_with_more_deps[tuple(model_dependencies)] = []
                tests_with_more_deps[tuple(model_dependencies)].append(node_name)

        for depends_on_tuple, test_node_names in tests_with_more_deps.items():
            test_names = [graph.nodes[test_node]["select"] for test_node in test_node_names]

            first_test_node = test_node_names[0]
            for test_node in test_node_names[1:]:
                nx.contracted_nodes(
                    graph, first_test_node, test_node, self_loops=False, copy=False  # in-memory
                )

            graph.nodes[first_test_node]["select"] = " ".join(test_names)
            nx.relabel_nodes(
                graph,
                {first_test_node: self._build_multiple_deps_test_name(depends_on_tuple)},
                copy=False,
            )

    def _create_tasks_from_graph(
        self, graph: nx.DiGraph, use_task_group: bool
    ) -> ModelExecutionTasks:
        starting_tasks = []
        ending_tasks = []
        result_tasks = {}

        for node_name, node in graph.nodes(data=True):
            if len(list(graph.predecessors(node_name))) == 0:
                starting_tasks.append(node_name)
            if len(list(graph.successors(node_name))) == 0:
                ending_tasks.append(node_name)

            if node["node_type"] == NodeType.MULTIPLE_DEPS_TEST:
                result_tasks[node_name] = ModelExecutionTask(
                    self._make_dbt_multiple_deps_test_task(node["select"], node_name), None
                )
            else:  # NodeType.RUN_TEST, NodeType.EPHEMERAL
                result_tasks[node_name] = self._create_task_for_model(
                    node["select"], node["node_type"] == NodeType.EPHEMERAL, use_task_group
                )

        for node, neighbour in graph.edges():
            # noinspection PyStatementEffect
            result_tasks[node].get_end_task() >> result_tasks[neighbour].get_start_task()

        return ModelExecutionTasks(result_tasks, list(starting_tasks), list(ending_tasks))

    def _make_dbt_tasks(
        self, manifest_path: str, use_task_group: bool, show_ephemeral_models: bool
    ) -> ModelExecutionTasks:
        manifest = self._load_dbt_manifest(manifest_path)

        graph = self._parse_manifest_into_graph(manifest)
        if not show_ephemeral_models:
            self._remove_ephemeral_nodes_from_graph(graph)
        self._contract_test_nodes(graph)

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
