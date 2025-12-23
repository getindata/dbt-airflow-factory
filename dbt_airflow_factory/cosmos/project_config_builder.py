"""Builder for Cosmos ProjectConfig from dbt.yml configuration."""

from typing import Any, Dict, Optional

from cosmos.config import ProjectConfig


def build_project_config(
    dbt_config: Dict[str, Any],
    manifest_path: Optional[str] = None,
    dag_id: Optional[str] = None,
) -> ProjectConfig:
    """
    Build Cosmos ProjectConfig from dbt.yml configuration.

    Uses explicit dbt_project_name from config, or falls back to dag_id.

    Args:
        dbt_config: Dictionary from dbt.yml containing:
            - project_dir_path: Path to dbt project directory (default: /dbt)
            - dbt_project_name: (optional) Explicit dbt project name
            - vars: (optional) dbt variables to pass through
        manifest_path: Optional path to manifest.json file
        dag_id: DAG ID, used as project name if dbt_project_name not specified

    Returns:
        Cosmos ProjectConfig instance

    Example dbt.yml:
        target: env_execution
        target_type: snowflake
        dbt_project_name: my_project  # Optional, defaults to dag_id
        vars:
          execution_ds: "{{ ds }}"
    """
    project_dir = dbt_config.get("project_dir_path", "/dbt")
    config_kwargs = {}

    if manifest_path:
        config_kwargs["manifest_path"] = manifest_path

    project_name = dbt_config.get("dbt_project_name") or dag_id
    if project_name:
        config_kwargs["project_name"] = project_name

    # Cosmos requires: either dbt_project_path OR (manifest + project_name)
    if not (manifest_path and project_name):
        config_kwargs["dbt_project_path"] = project_dir

    if "vars" in dbt_config and dbt_config["vars"]:
        config_kwargs["dbt_vars"] = dbt_config["vars"]

    return ProjectConfig(**config_kwargs)
