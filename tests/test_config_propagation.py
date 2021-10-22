from airflow.contrib.kubernetes.secret import Secret

from .utils import TEST_DAG, manifest_file_with_models, task_builder


def test_configuration():
    # given
    builder = task_builder()
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with TEST_DAG:
        tasks = builder.parse_manifest_into_tasks(manifest_path)

    # then
    run_task = tasks.get_task("model.dbt_test.dim_users").run_airflow_task
    assert run_task.namespace == "apache-airflow"
    assert run_task.image == "dbt-platform-poc:123"
    assert run_task.node_selectors == {"group": "data-processing"}
    assert run_task.tolerations == [
        {"key": "group", "value": "data-processing", "effect": "NoExecute"},
        {
            "key": "group",
            "operator": "Equal",
            "value": "data-processing",
            "effect": "NoSchedule",
        },
    ]

    assert run_task.labels == {"runner": "airflow"}
    assert run_task.secrets == [
        Secret("env", None, "snowflake-access-user-key", None),
        Secret("volume", "/var", "snowflake-access-user-key", None),
    ]
    assert run_task.is_delete_operator_pod
    assert "--project-dir /dbt" in run_task.arguments[0]
    assert "--profiles-dir /root/.dbt" in run_task.arguments[0]
    assert "--target dev" in run_task.arguments[0]
