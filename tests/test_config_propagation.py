from airflow.kubernetes.secret import Secret

from .utils import builder_factory, IS_FIRST_AIRFLOW_VERSION, manifest_file_with_models, test_dag


def test_configuration():
    # given
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with test_dag():
        tasks = builder_factory().create().parse_manifest_into_tasks(manifest_path)

    # then
    run_task = tasks.get_task("model.dbt_test.dim_users").run_airflow_task
    assert run_task.namespace == "apache-airflow"
    assert run_task.image == "123.gcr/dbt-platform-poc:123"

    if IS_FIRST_AIRFLOW_VERSION:
        assert run_task.node_selectors == {"group": "data-processing"}
        assert run_task.tolerations[0]["key"] == "group"
        assert run_task.tolerations[0]["operator"] == "Equal"
        assert run_task.tolerations[0]["value"] == "data-processing"
        assert run_task.tolerations[0]["effect"] == "NoSchedule"
        assert run_task.k8s_resources[0].limit_memory == "2048M"
        assert run_task.k8s_resources[0].limit_cpu == "2"
        assert run_task.k8s_resources[0].request_memory == "1024M"
        assert run_task.k8s_resources[0].request_cpu == "1"
    else:
        assert run_task.node_selector == {"group": "data-processing"}
        assert run_task.tolerations[0].key == "group"
        assert run_task.tolerations[0].operator == "Equal"
        assert run_task.tolerations[0].value == "data-processing"
        assert run_task.tolerations[0].effect == "NoSchedule"
        assert run_task.k8s_resources.limits == {"memory": "2048M", "cpu": "2"}
        assert run_task.k8s_resources.requests == {"memory": "1024M", "cpu": "1"}

    assert run_task.labels == {"runner": "airflow"}
    assert run_task.secrets == [
        Secret("env", "test", "snowflake-access-user-key", None),
        Secret("volume", "/var", "snowflake-access-user-key", None),
    ]
    assert run_task.is_delete_operator_pod
    assert "--project-dir /dbt" in run_task.arguments[0]
    assert "--profiles-dir /root/.dbt" in run_task.arguments[0]
    assert "--target dev" in run_task.arguments[0]
