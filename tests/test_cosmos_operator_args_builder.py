"""Unit tests for cosmos operator_args_builder module."""

from airflow.kubernetes.secret import Secret

from dbt_airflow_factory.cosmos.operator_args_builder import build_operator_args


def test_build_operator_args_empty():
    """Test building operator_args with no configuration."""
    # when
    result = build_operator_args()

    # then
    assert result == {}


def test_build_operator_args_k8s_passthrough():
    """Test transparent pass-through of k8s.yml configuration with resources flattening."""
    # given
    k8s_config = {
        "image_pull_policy": "IfNotPresent",
        "namespace": "apache-airflow",
        "envs": {
            "EXAMPLE_ENV": "example",
            "SECOND_EXAMPLE_ENV": "second",
        },
        "labels": {
            "runner": "airflow",
        },
        "annotations": {
            "iam.amazonaws.com/role": "k8s-airflow",
        },
        "is_delete_operator_pod": True,
        "resources": {
            "node_selectors": {
                "group": "data-processing",
            },
            "tolerations": [
                {
                    "key": "group",
                    "operator": "Equal",
                    "value": "data-processing",
                    "effect": "NoSchedule",
                }
            ],
            "limit": {
                "memory": "2048M",
                "cpu": "2",
            },
            "requests": {
                "memory": "1024M",
                "cpu": "1",
            },
        },
    }

    # when
    result = build_operator_args(k8s_config=k8s_config)

    # then - resources dict is flattened (backward compatibility with v0.35.0)
    assert result["image_pull_policy"] == "IfNotPresent"
    assert result["namespace"] == "apache-airflow"
    assert result["envs"]["EXAMPLE_ENV"] == "example"
    assert result["labels"]["runner"] == "airflow"
    assert "resources" not in result
    assert result["node_selectors"]["group"] == "data-processing"
    assert result["tolerations"][0]["key"] == "group"
    assert result["limit"]["memory"] == "2048M"
    assert result["requests"]["cpu"] == "1"


def test_build_operator_args_datahub_injection():
    """Test DataHub environment variable injection."""
    # given
    k8s_config = {
        "namespace": "apache-airflow",
        "envs": {
            "EXISTING_VAR": "value",
        },
    }
    datahub_config = {
        "datahub_gms_url": "http://datahub-gms:8080",
        "datahub_env_vars": {
            "DATAHUB_GMS_URL": "http://datahub-gms:8080",
            "DATAHUB_ENV": "PROD",
        },
    }

    # when
    result = build_operator_args(k8s_config=k8s_config, datahub_config=datahub_config)

    # then - DataHub envs merged with existing envs
    assert result["namespace"] == "apache-airflow"
    assert result["envs"]["EXISTING_VAR"] == "value"
    assert result["envs"]["DATAHUB_GMS_URL"] == "http://datahub-gms:8080"
    assert result["envs"]["DATAHUB_ENV"] == "PROD"


def test_build_operator_args_datahub_without_existing_envs():
    """Test DataHub injection when no existing envs."""
    # given
    k8s_config = {
        "namespace": "apache-airflow",
    }
    datahub_config = {
        "datahub_env_vars": {
            "DATAHUB_GMS_URL": "http://datahub-gms:8080",
        },
    }

    # when
    result = build_operator_args(k8s_config=k8s_config, datahub_config=datahub_config)

    # then - envs created with DataHub vars
    assert result["envs"] == {"DATAHUB_GMS_URL": "http://datahub-gms:8080"}


def test_build_operator_args_cosmos_overrides():
    """Test cosmos.yml operator_args overrides."""
    # given
    k8s_config = {
        "namespace": "apache-airflow",
        "image_pull_policy": "IfNotPresent",
    }
    cosmos_config = {
        "operator_args": {
            "install_deps": True,
            "full_refresh": False,
            "image_pull_policy": "Always",  # override k8s config
        },
    }

    # when
    result = build_operator_args(k8s_config=k8s_config, cosmos_config=cosmos_config)

    # then - cosmos overrides applied
    assert result["namespace"] == "apache-airflow"
    assert result["image_pull_policy"] == "Always"  # overridden
    assert result["install_deps"] is True
    assert result["full_refresh"] is False


def test_build_operator_args_cosmos_dict_merge():
    """Test cosmos.yml operator_args dictionary merging."""
    # given
    k8s_config = {
        "envs": {
            "VAR1": "value1",
            "VAR2": "value2",
        },
    }
    cosmos_config = {
        "operator_args": {
            "envs": {
                "VAR2": "overridden",  # override existing
                "VAR3": "value3",  # add new
            },
        },
    }

    # when
    result = build_operator_args(k8s_config=k8s_config, cosmos_config=cosmos_config)

    # then - envs merged
    assert result["envs"]["VAR1"] == "value1"
    assert result["envs"]["VAR2"] == "overridden"
    assert result["envs"]["VAR3"] == "value3"


def test_build_operator_args_comprehensive():
    """Test all features together."""
    # given
    k8s_config = {
        "namespace": "airflow",
        "image_pull_policy": "IfNotPresent",
        "envs": {
            "APP_ENV": "production",
        },
        "secrets": [
            {
                "secret": "my-secret",
                "deploy_type": "env",
                "deploy_target": "MY_SECRET",
            }
        ],
    }
    datahub_config = {
        "datahub_env_vars": {
            "DATAHUB_GMS_URL": "http://datahub:8080",
        },
    }
    cosmos_config = {
        "operator_args": {
            "install_deps": True,
            "envs": {
                "COSMOS_VAR": "cosmos_value",
            },
        },
    }

    # when
    result = build_operator_args(
        k8s_config=k8s_config,
        datahub_config=datahub_config,
        cosmos_config=cosmos_config,
    )

    # then
    assert result["namespace"] == "airflow"
    assert result["image_pull_policy"] == "IfNotPresent"
    assert result["install_deps"] is True
    # Secrets converted to Secret objects
    assert len(result["secrets"]) == 1
    secret = result["secrets"][0]
    assert isinstance(secret, Secret)
    assert secret.secret == "my-secret"
    assert secret.deploy_type == "env"
    assert secret.deploy_target == "MY_SECRET"
    # All envs merged
    assert result["envs"]["APP_ENV"] == "production"
    assert result["envs"]["DATAHUB_GMS_URL"] == "http://datahub:8080"
    assert result["envs"]["COSMOS_VAR"] == "cosmos_value"


def test_build_operator_args_only_datahub():
    """Test with only DataHub config."""
    # given
    datahub_config = {
        "datahub_env_vars": {
            "DATAHUB_GMS_URL": "http://datahub-gms:8080",
        },
    }

    # when
    result = build_operator_args(datahub_config=datahub_config)

    # then
    assert result == {"envs": {"DATAHUB_GMS_URL": "http://datahub-gms:8080"}}


def test_build_operator_args_only_cosmos():
    """Test with only Cosmos config."""
    # given
    cosmos_config = {
        "operator_args": {
            "install_deps": True,
            "full_refresh": False,
        },
    }

    # when
    result = build_operator_args(cosmos_config=cosmos_config)

    # then
    assert result == {"install_deps": True, "full_refresh": False}


def test_build_operator_args_docker_image_construction():
    """Test Docker image construction from execution_env.yml."""
    # given
    execution_env_config = {
        "type": "k8s",
        "image": {
            "repository": "gcr.io/my-project/dbt",
            "tag": "1.8.0",
        },
    }

    # when
    result = build_operator_args(execution_env_config=execution_env_config)

    # then
    assert result["image"] == "gcr.io/my-project/dbt:1.8.0"


def test_build_operator_args_docker_image_default_tag():
    """Test Docker image with default 'latest' tag."""
    # given
    execution_env_config = {
        "image": {
            "repository": "my-dbt-image",
        },
    }

    # when
    result = build_operator_args(execution_env_config=execution_env_config)

    # then
    assert result["image"] == "my-dbt-image:latest"


def test_build_operator_args_cosmos_dbt_flags():
    """Test Cosmos-specific dbt flags configuration."""
    # given
    cosmos_config = {
        "operator_args": {
            "dbt_executable_path": "/custom/dbt",
            "dbt_cmd_global_flags": ["--no-write-json", "--debug"],
            "dbt_cmd_flags": ["--full-refresh"],
        },
    }

    # when
    result = build_operator_args(cosmos_config=cosmos_config)

    # then
    assert result["dbt_executable_path"] == "/custom/dbt"
    assert result["dbt_cmd_global_flags"] == ["--no-write-json", "--debug"]
    assert result["dbt_cmd_flags"] == ["--full-refresh"]


def test_build_operator_args_secrets_conversion():
    """Test that secrets from k8s.yml are converted to Secret objects."""
    # given
    k8s_config = {
        "namespace": "airflow",
        "secrets": [
            {
                "secret": "snowflake-private-keys",
                "deploy_type": "env",
                "key": "dbt",
                "deploy_target": "SNOWFLAKE_DBT_PRIVATE_KEY",
            },
            {
                "secret": "datahub-auth",
                "deploy_type": "env",
                "deploy_target": "DATAHUB_TOKEN",
            },
        ],
    }

    # when
    result = build_operator_args(k8s_config=k8s_config)

    # then - secrets converted to Secret objects
    assert result["namespace"] == "airflow"
    assert "secrets" in result
    assert isinstance(result["secrets"], list)
    assert len(result["secrets"]) == 2

    # Check first secret
    secret1 = result["secrets"][0]
    assert isinstance(secret1, Secret)
    assert secret1.secret == "snowflake-private-keys"
    assert secret1.deploy_type == "env"
    assert secret1.key == "dbt"
    assert secret1.deploy_target == "SNOWFLAKE_DBT_PRIVATE_KEY"

    # Check second secret
    secret2 = result["secrets"][1]
    assert isinstance(secret2, Secret)
    assert secret2.secret == "datahub-auth"
    assert secret2.deploy_type == "env"
    assert secret2.deploy_target == "DATAHUB_TOKEN"


def test_build_operator_args_secrets_with_volume_mount():
    """Test secrets with volume deploy_type."""
    # given
    k8s_config = {
        "secrets": [
            {
                "secret": "ssh-keys",
                "deploy_type": "volume",
                "deploy_target": "/var/secrets",
            }
        ],
    }

    # when
    result = build_operator_args(k8s_config=k8s_config)

    # then
    assert len(result["secrets"]) == 1
    secret = result["secrets"][0]
    assert isinstance(secret, Secret)
    assert secret.secret == "ssh-keys"
    assert secret.deploy_type == "volume"
    assert secret.deploy_target == "/var/secrets"


def test_build_operator_args_no_secrets():
    """Test that missing secrets doesn't cause errors."""
    # given
    k8s_config = {
        "namespace": "airflow",
        "image_pull_policy": "IfNotPresent",
    }

    # when
    result = build_operator_args(k8s_config=k8s_config)

    # then - no secrets key
    assert "secrets" not in result
    assert result["namespace"] == "airflow"
