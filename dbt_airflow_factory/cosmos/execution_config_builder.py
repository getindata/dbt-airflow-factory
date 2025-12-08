"""Builder for Cosmos ExecutionConfig from execution_env.yml configuration."""

import logging
from typing import Any, Dict, Optional

from cosmos.config import ExecutionConfig
from cosmos.constants import ExecutionMode

logger = logging.getLogger(__name__)


def build_execution_config(
    execution_env_config: Dict[str, Any],
    dbt_project_path: str = "/dbt",
    cosmos_config: Optional[Dict[str, Any]] = None,
) -> ExecutionConfig:
    """
    Build Cosmos ExecutionConfig from execution_env.yml and cosmos.yml.

    Args:
        execution_env_config: Dictionary from execution_env.yml containing:
            - type: Execution environment type (k8s, bash, docker)
            - image: Docker image configuration
            - execution_script: Custom dbt executable path
        dbt_project_path: Path to dbt project directory
        cosmos_config: Optional cosmos.yml with execution_config section

    Returns:
        Cosmos ExecutionConfig instance

    Example execution_env.yml:
        type: k8s
        image:
          repository: gcr.io/my-project/dbt
          tag: "1.7.0"
        execution_script: ./custom_dbt_wrapper.sh

    Example cosmos.yml:
        execution_config:
          invocation_mode: subprocess
          test_indirect_selection: cautious

    Raises:
        ValueError: If execution type is not supported
    """
    exec_type = execution_env_config.get("type", "k8s").lower()

    type_mapping = {
        "k8s": ExecutionMode.KUBERNETES,
        "bash": ExecutionMode.LOCAL,
        "docker": ExecutionMode.DOCKER,
    }

    if exec_type not in type_mapping:
        raise ValueError(
            f"Unsupported execution type '{exec_type}'. "
            f"Supported types: {list(type_mapping.keys())}"
        )

    config_kwargs = {
        "execution_mode": type_mapping[exec_type],
        "dbt_project_path": dbt_project_path,
    }

    if "execution_script" in execution_env_config:
        exec_script = execution_env_config["execution_script"]
        config_kwargs["dbt_executable_path"] = exec_script
        logger.debug(f"Setting dbt_executable_path from execution_script: {exec_script}")

    if cosmos_config and "execution_config" in cosmos_config:
        allowed_fields = {
            "invocation_mode",
            "test_indirect_selection",
            "dbt_executable_path",
            "virtualenv_dir",
        }
        cosmos_exec_config = cosmos_config["execution_config"]
        config_kwargs.update({k: v for k, v in cosmos_exec_config.items() if k in allowed_fields})

    logger.debug(f"Creating ExecutionConfig with config_kwargs: {config_kwargs}")
    return ExecutionConfig(**config_kwargs)
