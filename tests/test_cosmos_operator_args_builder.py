"""Unit tests for cosmos operator_args_builder module."""

import warnings

from dbt_airflow_factory.cosmos.operator_args_builder import build_operator_args


def test_build_operator_args_empty():
    """Test building operator_args with no configuration."""
    # when
    result = build_operator_args()

    # then
    assert result == {}


def test_build_operator_args_k8s_passthrough():
    """Test transparent pass-through of k8s.yml configuration."""
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

    # then - entire config passed through
    assert result == k8s_config
    assert result["image_pull_policy"] == "IfNotPresent"
    assert result["namespace"] == "apache-airflow"
    assert result["envs"]["EXAMPLE_ENV"] == "example"
    assert result["labels"]["runner"] == "airflow"
    assert result["resources"]["node_selectors"]["group"] == "data-processing"


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
    assert result["secrets"][0]["secret"] == "my-secret"
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


def test_build_operator_args_execution_script_backward_compat():
    """Test backward compatibility with execution_script from execution_env.yml."""
    # given
    execution_env_config = {
        "type": "k8s",
        "execution_script": "/usr/local/bin/custom-dbt",
        "image": {
            "repository": "my-repo/dbt",
            "tag": "1.7.0",
        },
    }

    # when
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = build_operator_args(execution_env_config=execution_env_config)

        # then
        assert result["dbt_executable_path"] == "/usr/local/bin/custom-dbt"
        assert result["image"] == "my-repo/dbt:1.7.0"
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "execution_script" in str(w[0].message)
        assert "dbt_executable_path" in str(w[0].message)


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


def test_build_operator_args_execution_script_overridden_by_cosmos():
    """Test that cosmos.yml overrides deprecated execution_script."""
    # given
    execution_env_config = {
        "execution_script": "/old/dbt",
    }
    cosmos_config = {
        "operator_args": {
            "dbt_executable_path": "/new/dbt",
        },
    }

    # when
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        result = build_operator_args(
            execution_env_config=execution_env_config, cosmos_config=cosmos_config
        )

        # then - cosmos override wins
        assert result["dbt_executable_path"] == "/new/dbt"
