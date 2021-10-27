from airflow.kubernetes.secret import Secret

from .utils import manifest_file_with_models, task_builder, test_dag


def test_configuration():
    # given
    builder = task_builder()
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with test_dag():
        tasks = builder.parse_manifest_into_tasks(manifest_path)

    # then
    run_task = tasks.get_task("model.dbt_test.dim_users").run_airflow_task
    assert run_task.namespace == "apache-airflow"
    assert run_task.image == "dbt-platform-poc:123"
    assert run_task.node_selector == {"group": "data-processing"}
    assert run_task.tolerations[0].key == "group"
    assert run_task.tolerations[0].operator == "Equal"
    assert run_task.tolerations[0].value == "data-processing"
    assert run_task.tolerations[0].effect == "NoSchedule"
    assert run_task.labels == {"runner": "airflow"}
    assert run_task.secrets == [
        Secret("env", None, "snowflake-access-user-key", None),
        Secret("volume", "/var", "snowflake-access-user-key", None),
    ]
    assert run_task.is_delete_operator_pod
    assert "--project-dir /dbt" in run_task.arguments[0]
    assert "--profiles-dir /root/.dbt" in run_task.arguments[0]
    assert "--target dev" in run_task.arguments[0]
