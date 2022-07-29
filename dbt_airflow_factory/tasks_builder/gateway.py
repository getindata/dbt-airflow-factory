from typing import Any, Dict, List

from dbt_airflow_factory.tasks_builder.utils import is_model_run_task


def is_gateway_valid_dependency(
    dependency_node_name: str,
    manifest: dict,
    node: Dict[str, Any],
    dataset_left: str,
    dataset_right: str,
) -> bool:
    if is_model_run_task(dependency_node_name):
        dependency_node = manifest["nodes"][dependency_node_name]

        if dependency_node["schema"] == dataset_left and node["schema"] == dataset_right:
            return False
        return True
    return True


def get_gateway_dependencies(
    manifest: dict, separation_layer_left: str, separation_layer_right: str
) -> List:
    downstream_dependencies = _get_downstream_dependencies(
        manifest=manifest, separation_layer_right=separation_layer_right
    )

    upstream_dependencies_connected_to_downstream = (
        _get_upstream_dependencies_connected_to_downstream(
            manifest=manifest,
            separation_layer_left=separation_layer_left,
            downstream_dependencies=downstream_dependencies,
        )
    )
    dependencies = [
        node_name
        for node_name, values in manifest["nodes"].items()
        if values["schema"] == separation_layer_left
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
    manifest: dict, separation_layer_left: str, downstream_dependencies: List
) -> List:
    upstream_dependencies_connected_to_downstream = []

    for downstream_node in downstream_dependencies:
        upstream_deps = manifest["nodes"][downstream_node]["depends_on"]["nodes"]
        for dep in upstream_deps:
            _add_task(
                upstream_dependencies_connected_to_downstream=upstream_dependencies_connected_to_downstream,
                dep=dep,
                manifest=manifest,
                separation_layer_left=separation_layer_left
            )
            if is_model_run_task(dep) and manifest["nodes"][dep]["schema"] == separation_layer_left:
                upstream_dependencies_connected_to_downstream.append(dep)
    return upstream_dependencies_connected_to_downstream


def _add_task(
        upstream_dependencies_connected_to_downstream: List,
        dep: str,
        manifest: dict,
        separation_layer_left: str
):
    if is_model_run_task(dep) and manifest["nodes"][dep]["schema"] == separation_layer_left:
        upstream_dependencies_connected_to_downstream.append(dep)


def add_gateway_to_dependencies(
        gateway_name: str,
        filtered_dependencies: List[str],
        filtered_records: List[str]
):
    if len(filtered_dependencies) < len(filtered_records):
        filtered_dependencies.append(gateway_name)


def create_gateway_name(
        separation_layer_left: str, separation_layer_right: str, gateway_task_name: str
) -> str:
    return f"{separation_layer_left}_{separation_layer_right}_{gateway_task_name}"
