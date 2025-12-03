"""Builder for Cosmos operator_args using transparent pass-through strategy."""

import warnings
from typing import Any, Dict, Optional


def build_operator_args(
    k8s_config: Optional[Dict[str, Any]] = None,
    datahub_config: Optional[Dict[str, Any]] = None,
    cosmos_config: Optional[Dict[str, Any]] = None,
    execution_env_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build Cosmos operator_args using transparent pass-through strategy.

    This function implements a pass-through approach where the entire k8s.yml
    config dictionary is passed directly to Cosmos operator_args. Cosmos internally
    handles mapping these parameters to KubernetesPodOperator arguments.

    Performs transformations for:
    1. Docker image construction from execution_env.yml
    2. Backward compatibility: execution_script -> dbt_executable_path
    3. DataHub environment variable injection
    4. Cosmos-specific overrides from cosmos.yml

    Args:
        k8s_config: Complete dictionary from k8s.yml (passed through unchanged)
        datahub_config: Optional dictionary from datahub.yml for env var injection
        cosmos_config: Optional dictionary from cosmos.yml for override merging
        execution_env_config: Optional dictionary from execution_env.yml

    Returns:
        Dictionary to pass as operator_args to Cosmos DbtTaskGroup

    Example k8s.yml (all fields passed through):
        image_pull_policy: IfNotPresent
        namespace: apache-airflow
        envs:
          EXAMPLE_ENV: "example"
        secrets:
          - secret: my-secret
            deploy_type: env
            deploy_target: MY_SECRET
        labels:
          runner: airflow
        annotations:
          iam.amazonaws.com/role: "k8s-airflow"
        is_delete_operator_pod: True
        resources:
          node_selectors:
            group: data-processing
          tolerations:
            - key: group
              operator: Equal
              value: data-processing
          limit:
            memory: 2048M
            cpu: '2'
          requests:
            memory: 1024M
            cpu: '1'

    Example datahub.yml:
        datahub_gms_url: "http://datahub-gms:8080"
        datahub_env_vars:
          DATAHUB_GMS_URL: "http://datahub-gms:8080"
          DATAHUB_ENV: "PROD"

    Example cosmos.yml overrides (supports all Cosmos operator_args):
        operator_args:
          install_deps: true
          full_refresh: false
          dbt_executable_path: "/custom/path/to/dbt"
          dbt_cmd_global_flags:
            - "--no-write-json"
            - "--debug"
          dbt_cmd_flags:
            - "--full-refresh"

    Backward compatibility (DEPRECATED - see MIGRATION.md):
        execution_env.yml:
          type: k8s
          execution_script: "/usr/local/bin/dbt"  # Deprecated, migrate to cosmos.yml
    """
    # Start with empty operator_args
    operator_args = {}

    # 1. Add Docker image from execution_env if provided
    if execution_env_config and "image" in execution_env_config:
        image_config = execution_env_config["image"]
        repository = image_config.get("repository", "")
        tag = image_config.get("tag", "latest")
        if repository:
            operator_args["image"] = f"{repository}:{tag}"

    # 2. Backward compatibility: Map execution_script to dbt_executable_path
    if execution_env_config and "execution_script" in execution_env_config:
        operator_args["dbt_executable_path"] = execution_env_config["execution_script"]
        warnings.warn(
            "'execution_script' is deprecated. Use 'dbt_executable_path' in operator_args.",
            DeprecationWarning,
            stacklevel=2,
        )

    # 3. Pass through entire k8s.yml config if provided
    if k8s_config:
        operator_args.update(k8s_config)

    # 4. Inject DataHub environment variables if provided
    if datahub_config:
        # Merge DataHub env vars with existing envs
        datahub_envs = datahub_config.get("datahub_env_vars", {})
        if datahub_envs:
            if "envs" not in operator_args:
                operator_args["envs"] = {}  # type: ignore[assignment]
            # Type assertion for mypy
            envs_dict = operator_args["envs"]
            if isinstance(envs_dict, dict):
                envs_dict.update(datahub_envs)

    # 5. Merge cosmos.yml operator_args overrides if provided
    if cosmos_config and "operator_args" in cosmos_config:
        cosmos_overrides = cosmos_config["operator_args"]
        # Deep merge: cosmos overrides take precedence
        for key, value in cosmos_overrides.items():
            if key in operator_args:
                existing_value = operator_args[key]
                if isinstance(existing_value, dict) and isinstance(value, dict):
                    # Merge dictionaries
                    existing_value.update(value)
                else:
                    # Override directly
                    operator_args[key] = value
            else:
                # Add new key
                operator_args[key] = value

    return operator_args
