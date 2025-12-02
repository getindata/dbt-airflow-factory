"""Unit tests for cosmos project_config_builder module."""

from cosmos.config import ProjectConfig

from dbt_airflow_factory.cosmos.project_config_builder import build_project_config


def test_build_project_config_minimal():
    """Test building ProjectConfig with minimal configuration."""
    # given
    dbt_config = {
        "target": "env_execution",
        "target_type": "snowflake",
    }

    # when
    result = build_project_config(dbt_config)

    # then
    assert isinstance(result, ProjectConfig)
    assert str(result.dbt_project_path) == "/dbt"  # default


def test_build_project_config_with_custom_project_dir():
    """Test building ProjectConfig with custom project directory."""
    # given
    dbt_config = {
        "target": "local",
        "project_dir_path": "/custom/dbt/path",
    }

    # when
    result = build_project_config(dbt_config)

    # then
    assert str(result.dbt_project_path) == "/custom/dbt/path"


def test_build_project_config_with_project_name():
    """Test building ProjectConfig with explicit project name."""
    # given
    dbt_config = {
        "target": "env_execution",
        "project_dir_path": "/dbt",
        "dbt_project_name": "my_dbt_project",
    }

    # when
    result = build_project_config(dbt_config)

    # then
    assert str(result.dbt_project_path) == "/dbt"
    assert result.project_name == "my_dbt_project"


def test_build_project_config_with_manifest_path():
    """Test building ProjectConfig with manifest path."""
    # given
    dbt_config = {
        "target": "env_execution",
        "project_dir_path": "/dbt",
    }
    manifest_path = "/path/to/manifest.json"

    # when
    result = build_project_config(dbt_config, manifest_path=manifest_path)

    # then
    assert str(result.manifest_path) == manifest_path


def test_build_project_config_with_vars():
    """Test building ProjectConfig with dbt vars."""
    # given
    dbt_config = {
        "target": "env_execution",
        "project_dir_path": "/dbt",
        "vars": {
            "execution_ds": "{{ ds }}",
            "env": "production",
        },
    }

    # when
    result = build_project_config(dbt_config)

    # then
    assert result.dbt_vars == {
        "execution_ds": "{{ ds }}",
        "env": "production",
    }


def test_build_project_config_with_empty_vars():
    """Test building ProjectConfig with empty vars dict."""
    # given
    dbt_config = {
        "target": "env_execution",
        "project_dir_path": "/dbt",
        "vars": {},
    }

    # when
    result = build_project_config(dbt_config)

    # then
    # Empty vars should not be added to config
    assert not hasattr(result, "dbt_vars") or result.dbt_vars is None or result.dbt_vars == {}


def test_build_project_config_comprehensive():
    """Test building ProjectConfig with all parameters."""
    # given
    dbt_config = {
        "target": "env_execution",
        "target_type": "bigquery",
        "project_dir_path": "/opt/dbt",
        "dbt_project_name": "data_platform",
        "vars": {
            "start_date": "2024-01-01",
            "environment": "staging",
        },
    }
    manifest_path = "/opt/dbt/target/manifest.json"

    # when
    result = build_project_config(dbt_config, manifest_path=manifest_path)

    # then
    assert result.dbt_project_path is None  # Not set when manifest provided
    assert result.project_name == "data_platform"
    assert str(result.manifest_path) == manifest_path
    assert result.dbt_vars == {
        "start_date": "2024-01-01",
        "environment": "staging",
    }
