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
    assert "./test_script.sh" in run_task.arguments[0]
