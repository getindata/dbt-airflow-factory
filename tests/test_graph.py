from typing import Any, Dict

import pytest

from dbt_airflow_factory.tasks_builder.graph import (
    GatewayConfiguration,
    is_gateway_valid_dependency,
)

presentation_schema_name = "presentation"
staging_schema_name = "stg"
gateway_config = GatewayConfiguration(
    separation_schemas=[staging_schema_name, presentation_schema_name], gateway_task_name="gateway"
)


@pytest.mark.parametrize(
    "test_name,manifest,node,gateway_config,dependency_node_name,expected_value",
    [
        (
            "when stg element with different schema connected to presentation node, return False",
            {
                "nodes": {
                    "model.dim_user": {
                        "schema": presentation_schema_name,
                        "depends_on": {"nodes": ["model.stg_dim_user"]},
                    },
                    "model.stg_dim_user": {
                        "schema": staging_schema_name,
                        "depends_on": {"nodes": []},
                    },
                }
            },
            {"schema": presentation_schema_name, "depends_on": {"nodes": ["model.stg_dim_user"]}},
            gateway_config,
            "model.stg_dim_user",
            False,
        ),
        (
            "when two nodes are in the same schema (stg) should return True",
            {
                "nodes": {
                    "model.dim_user": {
                        "schema": staging_schema_name,
                        "depends_on": {"nodes": ["model.stg_dim_user"]},
                    },
                    "model.stg_dim_user": {
                        "schema": staging_schema_name,
                        "depends_on": {"nodes": []},
                    },
                }
            },
            {"schema": staging_schema_name, "depends_on": {"nodes": ["model.stg_dim_user"]}},
            gateway_config,
            "model.stg_dim_user",
            True,
        ),
        (
            "when two nodes are in the same schema (presentation) should return True",
            {
                "nodes": {
                    "model.dim_user": {
                        "schema": presentation_schema_name,
                        "depends_on": {"nodes": ["model.stg_dim_user"]},
                    },
                    "model.stg_dim_user": {
                        "schema": presentation_schema_name,
                        "depends_on": {"nodes": []},
                    },
                }
            },
            {"schema": presentation_schema_name, "depends_on": {"nodes": ["model.stg_dim_user"]}},
            gateway_config,
            "model.stg_dim_user",
            True,
        ),
        (
            "when node is not model type should return False",
            {
                "nodes": {
                    "model.dim_user": {
                        "schema": presentation_schema_name,
                        "depends_on": {"nodes": ["source.stg_dim_user"]},
                    },
                    "source.stg_dim_user": {
                        "schema": presentation_schema_name,
                        "depends_on": {"nodes": []},
                    },
                }
            },
            {"schema": presentation_schema_name, "depends_on": {"nodes": ["source.stg_dim_user"]}},
            gateway_config,
            "source.stg_dim_user",
            True,
        ),
    ],
)
def test_is_gateway_valid_dependency(
    test_name: str,
    manifest: dict,
    node: Dict[str, Any],
    dependency_node_name: str,
    gateway_config: GatewayConfiguration,
    expected_value: bool,
):
    is_valid_dependency = is_gateway_valid_dependency(
        dependency_node_name=dependency_node_name,
        manifest=manifest,
        node=node,
        dataset_left=gateway_config.separation_schemas[0],
        dataset_right=gateway_config.separation_schemas[1],
    )

    assert expected_value == is_valid_dependency
