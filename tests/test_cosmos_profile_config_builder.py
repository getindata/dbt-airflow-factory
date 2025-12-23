"""Unit tests for cosmos profile_config_builder module."""

from cosmos.config import ProfileConfig

from dbt_airflow_factory.cosmos.profile_config_builder import build_profile_config


def test_build_profile_config_minimal():
    """Test building ProfileConfig with minimal configuration."""
    # given
    dbt_config = {
        "target": "env_execution",
        "target_type": "snowflake",
    }

    # when
    result = build_profile_config(dbt_config)

    # then
    assert isinstance(result, ProfileConfig)
    assert result.profile_name == "snowflake"
    assert result.target_name == "env_execution"
    assert result.profiles_yml_filepath == "/root/.dbt/profiles.yml"


def test_build_profile_config_with_custom_profile_dir():
    """Test building ProfileConfig with custom profile directory."""
    # given
    dbt_config = {
        "target": "local",
        "profile_dir_path": "/custom/.dbt",
    }

    # when
    result = build_profile_config(dbt_config)

    # then
    assert result.profile_name == "local"
    assert result.target_name == "local"
    assert result.profiles_yml_filepath == "/custom/.dbt/profiles.yml"


def test_build_profile_config_with_default_target():
    """Test building ProfileConfig when target not provided."""
    # given
    dbt_config = {
        "target_type": "bigquery",
        "profile_dir_path": "/home/user/.dbt",
    }

    # when
    result = build_profile_config(dbt_config)

    # then
    assert result.profile_name == "bigquery"
    assert result.target_name == "local"
    assert result.profiles_yml_filepath == "/home/user/.dbt/profiles.yml"


def test_build_profile_config_comprehensive():
    """Test building ProfileConfig with all parameters."""
    # given
    dbt_config = {
        "target": "production",
        "target_type": "snowflake",
        "project_dir_path": "/dbt",
        "profile_dir_path": "/opt/airflow/dbt",
    }

    # when
    result = build_profile_config(dbt_config)

    # then
    assert result.profile_name == "snowflake"
    assert result.target_name == "production"
    assert result.profiles_yml_filepath == "/opt/airflow/dbt/profiles.yml"


def test_build_profile_config_with_explicit_profile_name():
    """Test building ProfileConfig with explicit profile_name override."""
    # given
    dbt_config = {
        "target": "env_execution",
        "target_type": "snowflake",
        "profile_name": "custom_profile",
        "profile_dir_path": "/root/.dbt",
    }

    # when
    result = build_profile_config(dbt_config)

    # then
    assert result.profile_name == "custom_profile"
    assert result.target_name == "env_execution"
    assert result.profiles_yml_filepath == "/root/.dbt/profiles.yml"
