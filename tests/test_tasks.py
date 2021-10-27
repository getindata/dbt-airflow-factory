from .utils import manifest_file_with_models, task_builder, test_dag


def test_get_dag():
    # given
    builder = task_builder()
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with test_dag():
        tasks = builder.parse_manifest_into_tasks(manifest_path)

    # then
    assert tasks.length() == 1
    assert tasks.get_task("model.dbt_test.dim_users") is not None
    assert tasks.get_task("model.dbt_test.dim_users").run_airflow_task is not None
    assert tasks.get_task("model.dbt_test.dim_users").test_airflow_task is not None


def test_run_task():
    # given
    builder = task_builder()
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with test_dag():
        tasks = builder.parse_manifest_into_tasks(manifest_path)

    # then
    run_task = tasks.get_task("model.dbt_test.dim_users").run_airflow_task
    assert run_task.cmds == ["bash", "-c"]
    assert "set -e; dbt --no-write-json run " in run_task.arguments[0]
    assert "--models dim_users" in run_task.arguments[0]
    assert run_task.name == "dim-users-run"
    assert run_task.task_id == "dim_users_run"


def test_test_task():
    # given
    builder = task_builder()
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with test_dag():
        tasks = builder.parse_manifest_into_tasks(manifest_path)

    # then
    test_task = tasks.get_task("model.dbt_test.dim_users").test_airflow_task
    assert test_task.cmds == ["bash", "-c"]
    assert "set -e; dbt --no-write-json test " in test_task.arguments[0]
    assert "--models dim_users" in test_task.arguments[0]
    assert test_task.name == "dim-users-test"
    assert test_task.task_id == "dim_users_test"
