"""Unit tests for cosmos config_translator module."""

from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import ExecutionMode

from dbt_airflow_factory.cosmos.config_translator import translate_configs


def test_translate_configs_minimal():
    """Test config translation with minimal configuration."""
    # given
    dbt_config = {
        "target": "env_execution",
        "target_type": "snowflake",
    }
    execution_env_config = {
        "type": "k8s",
        "image": {
            "repository": "gcr.io/my-project/dbt",
            "tag": "1.7.0",
        },
    }

    # when
    project_cfg, profile_cfg, exec_cfg, op_args = translate_configs(
        dbt_config=dbt_config,
        execution_env_config=execution_env_config,
    )

    # then
    assert isinstance(project_cfg, ProjectConfig)
    assert isinstance(profile_cfg, ProfileConfig)
    assert isinstance(exec_cfg, ExecutionConfig)
    assert isinstance(op_args, dict)

    assert str(project_cfg.dbt_project_path) == "/dbt"
    assert profile_cfg.target_name == "env_execution"
    assert exec_cfg.execution_mode == ExecutionMode.KUBERNETES
    assert op_args["image"] == "gcr.io/my-project/dbt:1.7.0"


def test_translate_configs_with_k8s():
    """Test config translation with Kubernetes configuration."""
    # given
    dbt_config = {
        "target": "production",
        "project_dir_path": "/opt/dbt",
    }
    execution_env_config = {
        "type": "k8s",
        "image": {
            "repository": "dbt-image",
            "tag": "v1.8.0",
        },
    }
    k8s_config = {
        "namespace": "airflow",
        "image_pull_policy": "Always",
        "envs": {
            "ENV": "prod",
        },
    }

    # when
    project_cfg, profile_cfg, exec_cfg, op_args = translate_configs(
        dbt_config=dbt_config,
        execution_env_config=execution_env_config,
        k8s_config=k8s_config,
    )

    # then
    assert str(project_cfg.dbt_project_path) == "/opt/dbt"
    assert profile_cfg.target_name == "production"
    assert exec_cfg.execution_mode == ExecutionMode.KUBERNETES
    # Image config handled via operator_args:"dbt-image:v1.8.0"
    assert op_args["namespace"] == "airflow"
    assert op_args["image_pull_policy"] == "Always"
    assert op_args["envs"]["ENV"] == "prod"


def test_translate_configs_with_datahub():
    """Test config translation with DataHub configuration."""
    # given
    dbt_config = {
        "target": "local",
    }
    execution_env_config = {
        "type": "bash",
    }
    datahub_config = {
        "datahub_env_vars": {
            "DATAHUB_GMS_URL": "http://datahub:8080",
        },
    }

    # when
    project_cfg, profile_cfg, exec_cfg, op_args = translate_configs(
        dbt_config=dbt_config,
        execution_env_config=execution_env_config,
        datahub_config=datahub_config,
    )

    # then
    assert exec_cfg.execution_mode == ExecutionMode.LOCAL
    assert op_args["envs"]["DATAHUB_GMS_URL"] == "http://datahub:8080"


def test_translate_configs_with_cosmos_overrides():
    """Test config translation with cosmos.yml overrides."""
    # given
    dbt_config = {
        "target": "env_execution",
    }
    execution_env_config = {
        "type": "k8s",
    }
    k8s_config = {
        "namespace": "airflow",
    }
    cosmos_config = {
        "load_mode": "dbt_manifest",
        "operator_args": {
            "install_deps": True,
        },
    }

    # when
    project_cfg, profile_cfg, exec_cfg, op_args = translate_configs(
        dbt_config=dbt_config,
        execution_env_config=execution_env_config,
        k8s_config=k8s_config,
        cosmos_config=cosmos_config,
    )

    # then
    assert op_args["namespace"] == "airflow"
    assert op_args["install_deps"] is True


def test_translate_configs_with_manifest_path():
    """Test config translation with manifest path."""
    # given
    dbt_config = {
        "target": "local",
    }
    execution_env_config = {
        "type": "k8s",
    }
    manifest_path = "/path/to/manifest.json"

    # when
    project_cfg, profile_cfg, exec_cfg, op_args = translate_configs(
        dbt_config=dbt_config,
        execution_env_config=execution_env_config,
        manifest_path=manifest_path,
    )

    # then
    assert str(project_cfg.manifest_path) == manifest_path


def test_translate_configs_comprehensive():
    """Test config translation with all parameters."""
    # given
    dbt_config = {
        "target": "production",
        "project_dir_path": "/opt/dbt",
        "profile_dir_path": "/opt/airflow/dbt",
        "dbt_project_name": "data_platform",
        "vars": {
            "execution_ds": "{{ ds }}",
        },
    }
    execution_env_config = {
        "type": "k8s",
        "image": {
            "repository": "gcr.io/project/dbt",
            "tag": "1.8.0",
        },
    }
    k8s_config = {
        "namespace": "data-platform",
        "image_pull_policy": "IfNotPresent",
        "envs": {
            "APP_ENV": "production",
        },
        "resources": {
            "limit": {
                "memory": "2048M",
            },
        },
    }
    datahub_config = {
        "datahub_env_vars": {
            "DATAHUB_GMS_URL": "http://datahub:8080",
            "DATAHUB_ENV": "PROD",
        },
    }
    cosmos_config = {
        "operator_args": {
            "install_deps": True,
            "envs": {
                "COSMOS_VAR": "value",
            },
        },
    }
    manifest_path = "/opt/dbt/target/manifest.json"

    # when
    project_cfg, profile_cfg, exec_cfg, op_args = translate_configs(
        dbt_config=dbt_config,
        execution_env_config=execution_env_config,
        k8s_config=k8s_config,
        datahub_config=datahub_config,
        cosmos_config=cosmos_config,
        manifest_path=manifest_path,
    )

    # then - ProjectConfig
    # NOTE: dbt_project_path not set when manifest provided (mutually exclusive with ExecutionConfig)
    assert project_cfg.dbt_project_path is None
    assert project_cfg.project_name == "data_platform"
    assert str(project_cfg.manifest_path) == manifest_path
    assert project_cfg.dbt_vars["execution_ds"] == "{{ ds }}"

    # then - ProfileConfig
    assert profile_cfg.profile_name == "production"
    assert profile_cfg.target_name == "production"
    assert profile_cfg.profiles_yml_filepath == "/opt/airflow/dbt/profiles.yml"

    # then - ExecutionConfig
    assert exec_cfg.execution_mode == ExecutionMode.KUBERNETES
    # Image config handled via operator_args:"gcr.io/project/dbt:1.8.0"

    # then - operator_args (all merged, resources flattened)
    assert op_args["namespace"] == "data-platform"
    assert op_args["image_pull_policy"] == "IfNotPresent"
    assert op_args["install_deps"] is True
    assert "resources" not in op_args
    assert op_args["limit"]["memory"] == "2048M"
    # All envs merged
    assert op_args["envs"]["APP_ENV"] == "production"
    assert op_args["envs"]["DATAHUB_GMS_URL"] == "http://datahub:8080"
    assert op_args["envs"]["DATAHUB_ENV"] == "PROD"
    assert op_args["envs"]["COSMOS_VAR"] == "value"
