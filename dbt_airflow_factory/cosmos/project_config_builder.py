"""Builder for Cosmos ProjectConfig from dbt.yml configuration."""

import json
import os
from typing import Any, Dict, Optional

from cosmos.config import ProjectConfig


def extract_project_name_from_manifest(manifest_path: str) -> Optional[str]:
    """
    Extract dbt project name from manifest.json.

    The project name is embedded in every node's unique ID with the format:
    node_type.project_name.node_name (e.g., "model.my_project.customers")

    Args:
        manifest_path: Path to manifest.json file

    Returns:
        Project name extracted from first node, or None if cannot extract
    """
    try:
        if not os.path.exists(manifest_path):
            return None

        with open(manifest_path, "r") as f:
            manifest = json.load(f)

        # Try nodes first (models, tests, snapshots, etc.)
        nodes = manifest.get("nodes", {})
        if nodes:
            first_node_id = next(iter(nodes.keys()))
            parts = first_node_id.split(".")
            if len(parts) >= 2:
                return parts[1]  # project_name is second part

        # Fallback: try sources
        sources = manifest.get("sources", {})
        if sources:
            first_source_id = next(iter(sources.keys()))
            parts = first_source_id.split(".")
            if len(parts) >= 2:
                return parts[1]

        return None

    except (json.JSONDecodeError, OSError, KeyError, StopIteration):
        return None


def build_project_config(
    dbt_config: Dict[str, Any],
    manifest_path: Optional[str] = None,
    dag_id: Optional[str] = None,
) -> ProjectConfig:
    """
    Build Cosmos ProjectConfig from dbt.yml configuration.

    Maps dbt-airflow-factory's dbt.yml format to Cosmos ProjectConfig.
    Validates that DAG ID matches dbt project name from manifest.

    Args:
        dbt_config: Dictionary from dbt.yml containing:
            - project_dir_path: Path to dbt project directory (default: /dbt)
            - dbt_project_name: (optional) Explicit name of the dbt project
            - vars: (optional) dbt variables to pass through
        manifest_path: Optional path to manifest.json file
        dag_id: Optional DAG ID for validation against project name

    Returns:
        Cosmos ProjectConfig instance

    Raises:
        ValueError: If DAG ID doesn't match project name and no explicit dbt_project_name set

    Example dbt.yml:
        target: env_execution
        target_type: snowflake
        project_dir_path: /dbt
        profile_dir_path: /root/.dbt
        dbt_project_name: my_project  # Explicit name (recommended if dag_id != project)
        vars:
          execution_ds: "{{ ds }}"
    """
    project_dir = dbt_config.get("project_dir_path", "/dbt")

    # Check if explicit project name is provided
    explicit_project_name = dbt_config.get("dbt_project_name")

    # Extract project name from manifest for validation
    manifest_project_name = None
    if manifest_path:
        manifest_project_name = extract_project_name_from_manifest(manifest_path)

    # Validate DAG ID matches project name
    if dag_id and manifest_project_name and not explicit_project_name:
        if dag_id != manifest_project_name:
            raise ValueError(
                f"DAG ID '{dag_id}' does not match dbt project name '{manifest_project_name}' "
                f"from manifest.json. Please add 'dbt_project_name: {manifest_project_name}' "
                f"to your config/base/dbt.yml to explicitly set the project name."
            )

    # Build ProjectConfig
    # NOTE: dbt_project_path is mutually exclusive with ExecutionConfig.dbt_project_path
    config_kwargs = {}

    # Add manifest path if available
    if manifest_path:
        config_kwargs["manifest_path"] = manifest_path

    # Add project name (explicit or extracted from manifest)
    project_name = explicit_project_name or manifest_project_name
    if project_name:
        config_kwargs["project_name"] = project_name

    # Cosmos requires: either dbt_project_path OR (manifest + project_name)
    # Set dbt_project_path if we don't have both manifest and name
    if not (manifest_path and project_name):
        config_kwargs["dbt_project_path"] = project_dir

    # Pass through dbt vars if provided
    if "vars" in dbt_config and dbt_config["vars"]:
        config_kwargs["dbt_vars"] = dbt_config["vars"]

    return ProjectConfig(**config_kwargs)
