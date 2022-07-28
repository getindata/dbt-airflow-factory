import itertools
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional

import networkx as nx

from dbt_airflow_factory.tasks_builder.node_type import NodeType
from dbt_airflow_factory.tasks_builder.utils import (
    is_ephemeral_task,
    is_model_run_task,
    is_source_sensor_task,
    is_test_task,
)


@dataclass
class GatewayConfiguration:
    separation_schemas: List[str]
    gateway_task_name: str


@dataclass
class TaskGraphConfiguration:
    gateway: Optional[GatewayConfiguration]


class DbtAirflowGraph:
    graph: nx.DiGraph

    def __init__(self, configuration: TaskGraphConfiguration) -> None:
        self.graph = nx.DiGraph()
        self.configuration = configuration

    def add_execution_tasks(self, manifest: dict) -> None:
        if self.configuration.gateway:
            for index, separation_layer in enumerate(self.configuration.gateway.separation_schemas[:-1]):
                separation_layers = self.configuration.gateway.separation_schemas
                separation_layer_left = separation_layers[index]
                separation_layer_right = separation_layers[index+1]
                self._add_gateway_node(
                    f"{separation_layer_left}_{separation_layer_right}_{self.configuration.gateway.gateway_task_name}", manifest,
                    separation_layer_left, separation_layer_right
                )

        for node_name, manifest_node in manifest["nodes"].items():
            if is_model_run_task(node_name):
                logging.info("Creating tasks for: " + node_name)
                self._add_graph_node_for_model_run_task(node_name, manifest_node, manifest)
            elif (
                is_test_task(node_name)
                and len(self._get_model_dependencies_from_manifest_node(manifest_node, manifest)) > 1
            ):
                logging.info("Creating tasks for: " + node_name)
                self._add_graph_node_for_multiple_deps_test(node_name, manifest_node, manifest)

    def add_external_dependencies(self, manifest: dict) -> None:
        manifest_child_map = manifest["child_map"]
        for source_name, manifest_source in manifest["sources"].items():
            if "dag" in manifest_source["source_meta"] and len(manifest_child_map[source_name]) > 0:
                logging.info("Creating source sensor for: " + source_name)
                self._add_sensor_source_node(source_name, manifest_source)

    def create_edges_from_dependencies(self, include_sensors: bool = False) -> None:
        for graph_node_name, graph_node in self.graph.nodes(data=True):
            for dependency in graph_node.get("depends_on", []):
                if not is_source_sensor_task(dependency) or include_sensors:
                    self.graph.add_edge(dependency, graph_node_name)

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

    def _add_execution_graph_node(
        self, node_name: str, manifest_node: Dict[str, Any], node_type: NodeType, manifest: dict
    ) -> None:
        self.graph.add_node(
            node_name,
            select=manifest_node["name"],
            depends_on=self._get_model_dependencies_from_manifest_node(manifest_node, manifest),
            node_type=node_type,
            target_schema=manifest_node["schema"],
        )

    def _add_sensor_source_node(self, node_name: str, manifest_node: Dict[str, Any]) -> None:
        self.graph.add_node(
            node_name,
            select=manifest_node["name"],
            dag=manifest_node["source_meta"]["dag"],
            node_type=NodeType.SOURCE_SENSOR,
        )

    def _add_gateway_node(self, node_name: str, manifest: dict,
                          separation_layer_left: str, separation_layer_right: str) -> None:
        self.graph.add_node(
            node_name,
            select=node_name,
            depends_on=self.get_downstream_dependencies(manifest, separation_layer_left, separation_layer_right),
            node_type=NodeType.MOCK_GATEWAY,
        )

    def get_downstream_dependencies(self, manifest: dict, separation_layer_left: str, separation_layer_right: str) -> List:
        downstream_dependencies = [
            node_name for node_name, values in manifest["nodes"].items()
            if values["schema"] == separation_layer_right
        ]

        upstream_dependencies_connected_to_downstream = []

        for downstream_node in downstream_dependencies:
            prod_deps = manifest["nodes"][downstream_node]["depends_on"]["nodes"]
            for dep in prod_deps:
                try:
                    if manifest["nodes"][dep]["schema"] == separation_layer_left:
                        upstream_dependencies_connected_to_downstream.append(dep)
                except Exception as e:
                    pass

        dependencies = [
            node_name for node_name, values in manifest["nodes"].items()
            if values["schema"] == separation_layer_left and
               node_name in upstream_dependencies_connected_to_downstream]
        return dependencies

    def _add_graph_node_for_model_run_task(
        self, node_name: str, manifest_node: Dict[str, Any], manifest: dict
    ) -> None:
        self._add_execution_graph_node(
            node_name,
            manifest_node,
            NodeType.EPHEMERAL if is_ephemeral_task(manifest_node) else NodeType.RUN_TEST,
            manifest
        )

    def _add_graph_node_for_multiple_deps_test(
        self, node_name: str, manifest_node: Dict[str, Any], manifest: dict
    ) -> None:
        self._add_execution_graph_node(node_name, manifest_node, NodeType.MULTIPLE_DEPS_TEST, manifest)

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

    def _get_model_dependencies_from_manifest_node(self, node: Dict[str, Any], manifest: dict) -> List[str]:
        filtered_records = list(filter(DbtAirflowGraph._is_valid_dependency, node["depends_on"]["nodes"]))

        if self.configuration.gateway:
            for index, schema_name in enumerate(self.configuration.gateway.separation_schemas[:-1]):
                separation_layers = self.configuration.gateway.separation_schemas
                separation_layer_left = separation_layers[index]
                separation_layer_right = separation_layers[index+1]

                filtered_records_by_s = list(filter(lambda dep_node: is_gateway_valid_dependency(
                    dependency_node_name=dep_node, manifest=manifest, node=node,
                    dataset_left=separation_layer_left,
                    dataset_right=separation_layer_right), filtered_records))

                if len(filtered_records_by_s) < len(filtered_records):
                    filtered_records_by_s.append(
                        f"{separation_layer_left}_{separation_layer_right}_{self.configuration.gateway.gateway_task_name}"

                    )
                return filtered_records_by_s

        return filtered_records

    @staticmethod
    def _is_valid_dependency(node_name: str) -> bool:
        is_valid_dep = is_model_run_task(node_name) or is_source_sensor_task(node_name)

        return is_valid_dep

    @staticmethod
    def _build_multiple_deps_test_name(dependencies: tuple) -> str:
        return "_".join((node_name.split(".")[-1] for node_name in dependencies)) + "_test"


def is_gateway_valid_dependency(dependency_node_name: str, manifest: dict, node: Dict[str, Any], dataset_left: str, dataset_right: str) -> bool:
    if is_model_run_task(dependency_node_name):
        dependency_node = manifest["nodes"][dependency_node_name]

        if dependency_node["schema"] == dataset_left and node["schema"] == dataset_right:
            return False
        else:
            return True
    return True
