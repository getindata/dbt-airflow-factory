"""Unit tests for cosmos execution_config_builder module."""

import pytest
from cosmos.config import ExecutionConfig
from cosmos.constants import ExecutionMode

from dbt_airflow_factory.cosmos.execution_config_builder import build_execution_config


def test_build_execution_config_k8s():
    """Test building ExecutionConfig for Kubernetes execution."""
    # given
    execution_env_config = {
        "type": "k8s",
        "image": {
            "repository": "gcr.io/my-project/dbt",
            "tag": "1.7.0",
        },
    }

    # when
    result = build_execution_config(execution_env_config)

    # then
    assert isinstance(result, ExecutionConfig)
    assert result.execution_mode == ExecutionMode.KUBERNETES
    # Note: Image is handled by operator_args, not ExecutionConfig


def test_build_execution_config_bash():
    """Test building ExecutionConfig for bash/local execution."""
    # given
    execution_env_config = {
        "type": "bash",
        "image": {
            "repository": "dbt-image",
            "tag": "latest",
        },
    }

    # when
    result = build_execution_config(execution_env_config)

    # then
    assert result.execution_mode == ExecutionMode.LOCAL
    # Image handled by operator_args:"dbt-image:latest"


def test_build_execution_config_docker():
    """Test building ExecutionConfig for Docker execution."""
    # given
    execution_env_config = {
        "type": "docker",
        "image": {
            "repository": "my-registry.io/dbt",
            "tag": "v1.8.0",
        },
    }

    # when
    result = build_execution_config(execution_env_config)

    # then
    assert result.execution_mode == ExecutionMode.DOCKER
    # Image handled by operator_args:"my-registry.io/dbt:v1.8.0"


def test_build_execution_config_without_image():
    """Test building ExecutionConfig without image specification."""
    # given
    execution_env_config = {
        "type": "k8s",
    }

    # when
    result = build_execution_config(execution_env_config)

    # then
    assert result.execution_mode == ExecutionMode.KUBERNETES
    # Should handle missing image gracefully


def test_build_execution_config_with_default_tag():
    """Test building ExecutionConfig with missing tag (should use latest)."""
    # given
    execution_env_config = {
        "type": "k8s",
        "image": {
            "repository": "gcr.io/my-project/dbt",
        },
    }

    # when
    result = build_execution_config(execution_env_config)

    # then
    assert result.execution_mode == ExecutionMode.KUBERNETES
    # Image handled by operator_args:"gcr.io/my-project/dbt:latest"


def test_build_execution_config_invalid_type():
    """Test building ExecutionConfig with unsupported type."""
    # given
    execution_env_config = {
        "type": "unsupported_type",
        "image": {
            "repository": "dbt-image",
            "tag": "1.0.0",
        },
    }

    # when/then
    with pytest.raises(ValueError) as exc_info:
        build_execution_config(execution_env_config)

    assert "Unsupported execution type" in str(exc_info.value)
    assert "unsupported_type" in str(exc_info.value)


def test_build_execution_config_case_insensitive():
    """Test that execution type is case insensitive."""
    # given
    execution_env_config = {
        "type": "K8S",  # uppercase
        "image": {
            "repository": "dbt-image",
            "tag": "1.0.0",
        },
    }

    # when
    result = build_execution_config(execution_env_config)

    # then
    assert result.execution_mode == ExecutionMode.KUBERNETES


def test_build_execution_config_default_type():
    """Test building ExecutionConfig with default type (k8s)."""
    # given
    execution_env_config = {
        "image": {
            "repository": "gcr.io/my-project/dbt",
            "tag": "1.7.0",
        },
    }

    # when
    result = build_execution_config(execution_env_config)

    # then
    assert result.execution_mode == ExecutionMode.KUBERNETES  # default


def test_build_execution_config_with_execution_script():
    """Test execution_script maps to dbt_executable_path."""
    # given
    execution_env_config = {
        "type": "k8s",
        "execution_script": "./executor_with_test_reports.sh",
    }

    # when
    result = build_execution_config(execution_env_config)

    # then
    assert result.execution_mode == ExecutionMode.KUBERNETES
    assert result.dbt_executable_path == "./executor_with_test_reports.sh"


def test_build_execution_config_cosmos_overrides_for_k8s():
    """Test cosmos.yml execution_config overrides for k8s mode."""
    # given
    execution_env_config = {"type": "k8s"}
    cosmos_config = {
        "execution_config": {
            "test_indirect_selection": "cautious",
            "dbt_executable_path": "/custom/dbt",
        }
    }

    # when
    result = build_execution_config(
        execution_env_config=execution_env_config,
        cosmos_config=cosmos_config,
    )

    # then
    assert result.execution_mode == ExecutionMode.KUBERNETES
    assert result.dbt_executable_path == "/custom/dbt"


def test_build_execution_config_cosmos_overrides_for_bash():
    """Test cosmos.yml execution_config overrides for bash/local mode."""
    # given
    execution_env_config = {"type": "bash"}
    cosmos_config = {
        "execution_config": {
            "invocation_mode": "subprocess",
            "dbt_executable_path": "/custom/dbt",
        }
    }

    # when
    result = build_execution_config(
        execution_env_config=execution_env_config,
        cosmos_config=cosmos_config,
    )

    # then
    assert result.execution_mode == ExecutionMode.LOCAL
    assert result.dbt_executable_path == "/custom/dbt"


def test_build_execution_config_cosmos_overrides_execution_script():
    """Test cosmos.yml dbt_executable_path overrides execution_script."""
    # given
    execution_env_config = {
        "type": "k8s",
        "execution_script": "/old/script.sh",
    }
    cosmos_config = {
        "execution_config": {
            "dbt_executable_path": "/new/script.sh",
        }
    }

    # when
    result = build_execution_config(
        execution_env_config=execution_env_config,
        cosmos_config=cosmos_config,
    )

    # then
    assert result.dbt_executable_path == "/new/script.sh"
