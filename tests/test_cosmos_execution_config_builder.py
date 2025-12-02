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
