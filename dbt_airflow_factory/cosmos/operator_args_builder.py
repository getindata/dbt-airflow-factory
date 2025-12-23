"""Builder for Cosmos operator_args using transparent pass-through strategy."""

from typing import Any, Dict, List, Optional, cast

from airflow.kubernetes.secret import Secret


def _convert_secrets_to_objects(secrets_config: List[Dict[str, Any]]) -> List[Secret]:
    """
    Convert secrets from dictionary format to Airflow Secret objects.

    The k8s.yml uses dictionary format for secrets:
        secrets:
          - secret: my-secret-name
            deploy_type: env
            deploy_target: MY_ENV_VAR
            key: optional-key

    Cosmos/KubernetesPodOperator expects Secret objects from Airflow.

    Args:
        secrets_config: List of secret dictionaries from k8s.yml

    Returns:
        List of Airflow Secret objects
    """
    return [Secret(**secret) for secret in secrets_config]


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
    operator_args = {}

    # 1. Add Docker image from execution_env if provided
    if execution_env_config and "image" in execution_env_config:
        image_config = execution_env_config["image"]
        repository = image_config.get("repository", "")
        tag = image_config.get("tag", "latest")
        if repository:
            operator_args["image"] = f"{repository}:{tag}"

    # 2. Pass through entire k8s.yml config if provided
    if k8s_config:
        operator_args.update(k8s_config)

    # 2a. Flatten resources dict (backward compatibility with v0.35.0)
    if "resources" in operator_args and isinstance(operator_args["resources"], dict):
        resources = cast(Dict[str, Any], operator_args.pop("resources"))
        operator_args.update(resources)

    # 2b. Convert secrets from dictionary format to Secret objects
    if "secrets" in operator_args and isinstance(operator_args["secrets"], list):
        secrets_list = operator_args["secrets"]
        if secrets_list and isinstance(secrets_list[0], dict):
            operator_args["secrets"] = _convert_secrets_to_objects(secrets_list)

    # 3. Inject DataHub environment variables if provided
    if datahub_config:
        datahub_envs = datahub_config.get("datahub_env_vars", {})
        if datahub_envs:
            # Use setdefault to get or create envs dict
            existing_envs = operator_args.setdefault("envs", cast(Any, {}))
            if isinstance(existing_envs, dict):
                existing_envs.update(datahub_envs)

    # 4. Merge cosmos.yml operator_args section
    if cosmos_config and "operator_args" in cosmos_config:
        cosmos_op_args = cosmos_config["operator_args"]
        for key, value in cosmos_op_args.items():
            if key in operator_args:
                existing_value = operator_args[key]
                if isinstance(existing_value, dict) and isinstance(value, dict):
                    existing_value.update(value)
                else:
                    operator_args[key] = value
            else:
                operator_args[key] = value

    return operator_args
