import itertools
import logging
from typing import Any, Dict, List, Tuple

import networkx as nx

from dbt_airflow_factory.tasks_builder.gateway import (
    NodeProperties,
    SeparationLayer,
    TaskGraphConfiguration,
    add_gateway_to_dependencies,
    create_gateway_name,
    get_gateway_dependencies,
    is_gateway_valid_dependency,
    should_gateway_be_added,
)
from dbt_airflow_factory.tasks_builder.node_type import NodeType
from dbt_airflow_factory.tasks_builder.utils import (
    is_ephemeral_task,
    is_model_run_task,
    is_source_sensor_task,
    is_test_task,
)


class DbtAirflowGraph:
    graph: nx.DiGraph

    def __init__(self, configuration: TaskGraphConfiguration) -> None:
        self.graph = nx.DiGraph()
        self.configuration = configuration

    def add_execution_tasks(self, manifest: dict) -> None:
        self._add_gateway_execution_tasks(manifest=manifest)

        for node_name, manifest_node in manifest["nodes"].items():
            if is_model_run_task(node_name):
                logging.info("Creating tasks for: " + node_name)
                self._add_graph_node_for_model_run_task(node_name, manifest_node, manifest)
            elif (
                is_test_task(node_name)
                and len(self._get_model_dependencies_from_manifest_node(manifest_node, manifest))
                > 1
            ):
                logging.info("Creating tasks for: " + node_name)
                self._add_graph_node_for_multiple_deps_test(node_name, manifest_node, manifest)

    def _add_gateway_execution_tasks(self, manifest: dict) -> None:
        if self.configuration.gateway.separation_schemas.__len__() >= 2:
            separation_layers = self.configuration.gateway.separation_schemas

            for index, _ in enumerate(separation_layers[:-1]):
                separation_layer_left = separation_layers[index]
                separation_layer_right = separation_layers[index + 1]
                self._add_gateway_node(
                    manifest=manifest,
                    separation_layer=SeparationLayer(
                        left=separation_layer_left, right=separation_layer_right
                    ),
                )

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
                    list(self.graph.predecessors(node_name)),
                    list(self.graph.successors(node_name)),
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
        )

    def _add_sensor_source_node(self, node_name: str, manifest_node: Dict[str, Any]) -> None:
        self.graph.add_node(
            node_name,
            select=manifest_node["name"],
            dag=manifest_node["source_meta"]["dag"],
            node_type=NodeType.SOURCE_SENSOR,
        )

    def _add_gateway_node(self, manifest: dict, separation_layer: SeparationLayer) -> None:
        node_name = create_gateway_name(
            separation_layer=separation_layer,
            gateway_task_name=self.configuration.gateway.gateway_task_name,
        )
        self.graph.add_node(
            node_name,
            select=node_name,
            depends_on=get_gateway_dependencies(
                separation_layer=separation_layer, manifest=manifest
            ),
            node_type=NodeType.MOCK_GATEWAY,
        )

    def _add_graph_node_for_model_run_task(
        self, node_name: str, manifest_node: Dict[str, Any], manifest: dict
    ) -> None:
        self._add_execution_graph_node(
            node_name,
            manifest_node,
            NodeType.EPHEMERAL if is_ephemeral_task(manifest_node) else NodeType.RUN_TEST,
            manifest,
        )

    def _add_graph_node_for_multiple_deps_test(
        self, node_name: str, manifest_node: Dict[str, Any], manifest: dict
    ) -> None:
        self._add_execution_graph_node(
            node_name, manifest_node, NodeType.MULTIPLE_DEPS_TEST, manifest
        )

    def _get_test_with_multiple_deps_names_by_deps(
        self,
    ) -> Dict[Tuple[str, ...], List[str]]:
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
                self.graph,
                first_test_node,
                test_node,
                self_loops=False,
                copy=False,  # in-memory
            )

        self.graph.nodes[first_test_node]["select"] = " ".join(test_names)
        nx.relabel_nodes(
            self.graph,
            {first_test_node: self._build_multiple_deps_test_name(depends_on_tuple)},
            copy=False,
        )

    def _get_model_dependencies_from_manifest_node(
        self, node: Dict[str, Any], manifest: dict
    ) -> List[str]:
        filtered_records = list(
            filter(DbtAirflowGraph._is_valid_dependency, node["depends_on"]["nodes"])
        )
        node_schema = node.get("schema", None)

        if should_gateway_be_added(
            node_schema=node_schema,
            separation_schemas=self.configuration.gateway.separation_schemas,
        ):
            node_schema_index = self.configuration.gateway.separation_schemas.index(node_schema)
            if node_schema_index >= 1:
                filtered_records = self._filter_to_gateway_conditions(
                    node_schema_index=node_schema_index,
                    manifest=manifest,
                    node=node,
                    filtered_records=filtered_records,
                )

        return filtered_records

    def _filter_to_gateway_conditions(
        self,
        node_schema_index: int,
        manifest: dict,
        node: Dict[str, Any],
        filtered_records: List[str],
    ) -> List[str]:
        separation_layers = self.configuration.gateway.separation_schemas
        separation_layer_left = separation_layers[node_schema_index - 1]
        separation_layer_right = separation_layers[node_schema_index]

        filtered_dependencies = list(
            filter(
                lambda dep_node: is_gateway_valid_dependency(
                    separation_layer=SeparationLayer(
                        left=separation_layer_left, right=separation_layer_right
                    ),
                    dependency_node_properties=NodeProperties(
                        node_name=dep_node,
                        schema_name=manifest["nodes"][dep_node]["schema"],
                    ),
                    node_schema=node["schema"],
                ),
                filtered_records,
            )
        )

        add_gateway_to_dependencies(
            filtered_dependencies=filtered_dependencies,
            filtered_records=filtered_records,
            gateway_name=create_gateway_name(
                separation_layer=SeparationLayer(
                    left=separation_layer_left, right=separation_layer_right
                ),
                gateway_task_name=self.configuration.gateway.gateway_task_name,
            ),
        )
        return filtered_dependencies

    @staticmethod
    def _is_valid_dependency(node_name: str) -> bool:
        return is_model_run_task(node_name) or is_source_sensor_task(node_name)

    @staticmethod
    def _build_multiple_deps_test_name(dependencies: tuple) -> str:
        return "_".join((node_name.split(".")[-1] for node_name in dependencies)) + "_test"
