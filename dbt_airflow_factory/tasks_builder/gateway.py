from dataclasses import dataclass
from typing import List

from dbt_airflow_factory.tasks_builder.utils import is_model_run_task


@dataclass
class NodeProperties:
    node_name: str
    schema_name: str


@dataclass
class GatewayConfiguration:
    separation_schemas: List[str]
    gateway_task_name: str


@dataclass
class TaskGraphConfiguration:
    gateway: GatewayConfiguration


@dataclass
class SeparationLayer:
    left: str
    right: str


def is_gateway_valid_dependency(
    separation_layer: SeparationLayer, dependency_node_properties: NodeProperties, node_schema: str
) -> bool:
    if is_model_run_task(dependency_node_properties.node_name):
        dep_schema = dependency_node_properties.schema_name
        if dep_schema == separation_layer.left and node_schema == separation_layer.right:
            return False
        return True
    return True


def get_gateway_dependencies(manifest: dict, separation_layer: SeparationLayer) -> List:
    downstream_dependencies = _get_downstream_dependencies(
        manifest=manifest, separation_layer_right=separation_layer.right
    )

    upstream_dependencies_connected_to_downstream = (
        _get_upstream_dependencies_connected_to_downstream(
            manifest=manifest,
            separation_layer_left=separation_layer.left,
            downstream_dependencies=downstream_dependencies,
        )
    )
    dependencies = [
        node_name
        for node_name, values in manifest["nodes"].items()
        if values["schema"] == separation_layer.left
        and node_name in upstream_dependencies_connected_to_downstream
    ]
    return dependencies


def _get_downstream_dependencies(manifest: dict, separation_layer_right: str) -> List:
    downstream_dependencies = [
        node_name
        for node_name, values in manifest["nodes"].items()
        if values["schema"] == separation_layer_right
    ]
    return downstream_dependencies


def _get_upstream_dependencies_connected_to_downstream(
    manifest: dict, separation_layer_left: str, downstream_dependencies: List[str]
) -> List:
    upstream_deps_connected_to_downstream: List[str] = []

    for downstream_node in downstream_dependencies:
        upstream_deps = manifest["nodes"][downstream_node]["depends_on"]["nodes"]
        for dep in upstream_deps:
            _add_upstream_dep_based_on_downstream(
                dep=dep,
                manifest=manifest,
                separation_layer_left=separation_layer_left,
                upstream_dependencies_connected_to_downstream=upstream_deps_connected_to_downstream,
            )
    return upstream_deps_connected_to_downstream


def _add_upstream_dep_based_on_downstream(
    dep: str,
    manifest: dict,
    separation_layer_left: str,
    upstream_dependencies_connected_to_downstream: List[str],
) -> None:
    if is_model_run_task(dep) and manifest["nodes"][dep]["schema"] == separation_layer_left:
        upstream_dependencies_connected_to_downstream.append(dep)


def add_gateway_to_dependencies(
    gateway_name: str, filtered_dependencies: List[str], filtered_records: List[str]
) -> None:
    if len(filtered_dependencies) < len(filtered_records):
        filtered_dependencies.append(gateway_name)


def create_gateway_name(separation_layer: SeparationLayer, gateway_task_name: str) -> str:
    return f"{separation_layer.left}_{separation_layer.right}_{gateway_task_name}"


def should_gateway_be_added(node_schema: str, separation_schemas: List[str]) -> bool:
    valid_schemas_input_length = len(separation_schemas) >= 2
    schema_is_in_given_schemas = node_schema in separation_schemas
    return valid_schemas_input_length and schema_is_in_given_schemas
