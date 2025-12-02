"""Builder for Cosmos ExecutionConfig from execution_env.yml configuration."""

from typing import Any, Dict

from cosmos.config import ExecutionConfig
from cosmos.constants import ExecutionMode


def build_execution_config(
    execution_env_config: Dict[str, Any], dbt_project_path: str = "/dbt"
) -> ExecutionConfig:
    """
    Build Cosmos ExecutionConfig from execution_env.yml configuration.

    Maps dbt-airflow-factory's execution environment type to Cosmos ExecutionMode.

    Args:
        execution_env_config: Dictionary from execution_env.yml containing:
            - type: Execution environment type (k8s, bash, docker, ecs)
            - image: Docker image configuration with repository and tag
        dbt_project_path: Path to dbt project directory for task execution

    Returns:
        Cosmos ExecutionConfig instance

    Example execution_env.yml:
        image:
          repository: gcr.io/my-project/dbt
          tag: "1.7.0"
        type: k8s

    Raises:
        ValueError: If execution type is not supported
    """
    exec_type = execution_env_config.get("type", "k8s").lower()

    # Map dbt-airflow-factory execution types to Cosmos ExecutionMode
    type_mapping = {
        "k8s": ExecutionMode.KUBERNETES,
        "bash": ExecutionMode.LOCAL,
        "docker": ExecutionMode.DOCKER,
        # Note: ECS support depends on Cosmos version
        # Cosmos 1.11+ may support AWS_ECS via KubernetesPodOperator compatibility
    }

    if exec_type not in type_mapping:
        raise ValueError(
            f"Unsupported execution type '{exec_type}'. "
            f"Supported types: {list(type_mapping.keys())}"
        )

    execution_mode = type_mapping[exec_type]

    # Create ExecutionConfig with execution mode and project path
    # dbt_project_path is required for Cosmos to execute dbt commands
    return ExecutionConfig(execution_mode=execution_mode, dbt_project_path=dbt_project_path)
