from airflow.kubernetes.secret import Secret

from .utils import builder_factory, manifest_file_with_models, test_dag


def test_configuration_with_qa_config():
    # given
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with test_dag():
        tasks = builder_factory(env="qa").create().parse_manifest_into_tasks(manifest_path)

    # then
    run_task = tasks.get_task("model.dbt_test.dim_users").execution_airflow_task
    assert run_task.env_vars[2].to_dict() == {
        "name": "DATAHUB_GMS_URL",
        "value": "http://test_url:8080",
        "value_from": None,
    }
    assert "./executor_with_test_reports_ingestions.sh" in run_task.arguments[0]
    assert run_task.secrets == [
        Secret("env", "test", "snowflake-access-user-key", None),
        Secret("volume", "/var", "snowflake-access-user-key", None),
    ]
    assert run_task.in_cluster is False
    assert run_task.cluster_context == "test"
    assert run_task.startup_timeout_seconds == 600
