"""Main config translator for converting dbt-airflow-factory configs to Cosmos."""

from typing import Any, Dict, Optional, Tuple

from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig

from dbt_airflow_factory.cosmos.execution_config_builder import build_execution_config
from dbt_airflow_factory.cosmos.operator_args_builder import build_operator_args
from dbt_airflow_factory.cosmos.profile_config_builder import build_profile_config
from dbt_airflow_factory.cosmos.project_config_builder import build_project_config


def translate_configs(
    dbt_config: Dict[str, Any],
    execution_env_config: Dict[str, Any],
    k8s_config: Optional[Dict[str, Any]] = None,
    datahub_config: Optional[Dict[str, Any]] = None,
    cosmos_config: Optional[Dict[str, Any]] = None,
    manifest_path: Optional[str] = None,
    dag_id: Optional[str] = None,
) -> Tuple[ProjectConfig, ProfileConfig, ExecutionConfig, Dict[str, Any]]:
    """
    Translate dbt-airflow-factory configuration files to Cosmos config objects.

    This is the main entry point for config translation. It reads the YAML-based
    configurations and converts them to Cosmos-compatible format.

    Args:
        dbt_config: Dictionary from dbt.yml (target, project_dir_path, profile_dir_path, vars)
        execution_env_config: Dictionary from execution_env.yml (type, image)
        k8s_config: Optional dictionary from k8s.yml (passed through to operator_args)
        datahub_config: Optional dictionary from datahub.yml (for env var injection)
        cosmos_config: Optional dictionary from cosmos.yml (for advanced Cosmos features)
        manifest_path: Optional path to manifest.json file
        dag_id: Optional DAG ID for validation against dbt project name

    Returns:
        Tuple of (ProjectConfig, ProfileConfig, ExecutionConfig, operator_args dict)

    Example Usage:
        ```python
        from dbt_airflow_factory.config_utils import read_config
        from dbt_airflow_factory.cosmos import translate_configs

        # Read config files
        dbt_config = read_config(dag_path, env, "dbt.yml")
        exec_env_config = read_config(dag_path, env, "execution_env.yml")
        k8s_config = read_config(dag_path, env, "k8s.yml")
        datahub_config = read_config(dag_path, env, "datahub.yml", optional=True)
        cosmos_config = read_config(dag_path, env, "cosmos.yml", optional=True)

        # Translate to Cosmos configs
        project_cfg, profile_cfg, exec_cfg, op_args = translate_configs(
            dbt_config=dbt_config,
            execution_env_config=exec_env_config,
            k8s_config=k8s_config,
            datahub_config=datahub_config,
            cosmos_config=cosmos_config,
            manifest_path="/path/to/manifest.json"
        )

        # Use with Cosmos DbtTaskGroup
        from cosmos import DbtTaskGroup

        dbt_tg = DbtTaskGroup(
            group_id="dbt",
            project_config=project_cfg,
            profile_config=profile_cfg,
            execution_config=exec_cfg,
            operator_args=op_args,
        )
        ```

    Configuration Files Structure:
        - dbt.yml: dbt project and profile settings
        - execution_env.yml: execution environment (k8s, bash, docker)
        - k8s.yml: Kubernetes pod configuration (passed through transparently)
        - datahub.yml: (optional) DataHub environment variables
        - cosmos.yml: (optional) Advanced Cosmos-specific overrides
    """
    # Build Cosmos config objects from YAML configs
    project_config = build_project_config(
        dbt_config=dbt_config, manifest_path=manifest_path, dag_id=dag_id
    )

    profile_config = build_profile_config(dbt_config=dbt_config)

    dbt_project_path = dbt_config.get("project_dir_path", "/dbt")
    execution_config = build_execution_config(
        execution_env_config=execution_env_config,
        dbt_project_path=dbt_project_path,
        cosmos_config=cosmos_config,
    )

    operator_args = build_operator_args(
        k8s_config=k8s_config,
        datahub_config=datahub_config,
        cosmos_config=cosmos_config,
        execution_env_config=execution_env_config,
    )

    return project_config, profile_config, execution_config, operator_args
