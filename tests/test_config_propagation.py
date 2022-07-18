from airflow.kubernetes.secret import Secret

from .utils import (
    IS_FIRST_AIRFLOW_VERSION,
    builder_factory,
    manifest_file_with_models,
    test_dag,
)


def test_configuration():
    # given
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with test_dag():
        tasks = builder_factory().create().parse_manifest_into_tasks(manifest_path)

    # then
    run_task = tasks.get_task("model.dbt_test.dim_users").execution_airflow_task
    assert run_task.namespace == "apache-airflow"
    assert run_task.image == "123.gcr/dbt-platform-poc:123"
    if IS_FIRST_AIRFLOW_VERSION:
        assert run_task.node_selectors == {"group": "data-processing"}
        assert run_task.tolerations[0]["key"] == "group"
        assert run_task.tolerations[0]["operator"] == "Equal"
        assert run_task.tolerations[0]["value"] == "data-processing"
        assert run_task.tolerations[0]["effect"] == "NoSchedule"
        assert run_task.resources[0].limit_memory == "2048M"
        assert run_task.resources[0].limit_cpu == "2"
        assert run_task.resources[0].request_memory == "1024M"
        assert run_task.resources[0].request_cpu == "1"
    else:
        assert run_task.node_selector == {"group": "data-processing"}
        assert run_task.tolerations[0].key == "group"
        assert run_task.tolerations[0].operator == "Equal"
        assert run_task.tolerations[0].value == "data-processing"
        assert run_task.tolerations[0].effect == "NoSchedule"
        assert run_task.k8s_resources.limits == {"memory": "2048M", "cpu": "2"}
        assert run_task.k8s_resources.requests == {"memory": "1024M", "cpu": "1"}

    assert run_task.labels == {"runner": "airflow"}
    assert run_task.env_vars[0].to_dict() == {
        "name": "EXAMPLE_ENV",
        "value": "example",
        "value_from": None,
    }
    assert run_task.env_vars[1].to_dict() == {
        "name": "SECOND_EXAMPLE_ENV",
        "value": "second",
        "value_from": None,
    }
    assert run_task.secrets == [
        Secret("env", "test", "snowflake-access-user-key", None),
        Secret("volume", "/var", "snowflake-access-user-key", None),
    ]
    assert (
        run_task.arguments[0]
        == 'set -e; dbt --no-write-json run --target dev --vars "{}" --project-dir /dbt --profiles-dir /root/.dbt --select dim_users'  # noqa: E501
    )
    assert run_task.config_file == "/usr/local/airflow/dags/kube_config.yaml"
    assert run_task.is_delete_operator_pod
    assert "--project-dir /dbt" in run_task.arguments[0]
    assert "--profiles-dir /root/.dbt" in run_task.arguments[0]
    assert "--target dev" in run_task.arguments[0]


def test_configuration_with_qa_config():
    # given
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with test_dag():
        tasks = builder_factory(env="qa").create().parse_manifest_into_tasks(manifest_path)

    # then
    run_task = tasks.get_task("model.dbt_test.dim_users").execution_airflow_task
    assert (
        run_task.arguments[0]
        == './executor_with_test_reports_ingestions.sh dbt --no-write-json run --target qa --vars "{}" --project-dir /dbt --profiles-dir /root/.dbt --select dim_users'  # noqa: E501
    )
    assert run_task.env_vars[2].to_dict() == {
        "name": "DATAHUB_GMS_URL",
        "value": "http://test_url:8080",
        "value_from": None,
    }
