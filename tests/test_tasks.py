from os import path

from dbt_airflow_manifest_parser.builder_factory import DbtAirflowTasksBuilderFactory

from .utils import (
    IS_FIRST_AIRFLOW_VERSION,
    builder_factory,
    manifest_file_with_models,
    task_group_prefix_builder,
    test_dag,
)


def test_get_dag():
    # given
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with test_dag():
        tasks = builder_factory().create().parse_manifest_into_tasks(manifest_path)

    # then
    assert tasks.length() == 1
    assert tasks.get_task("model.dbt_test.dim_users") is not None
    assert tasks.get_task("model.dbt_test.dim_users").run_airflow_task is not None
    assert tasks.get_task("model.dbt_test.dim_users").test_airflow_task is not None


def test_run_task():
    # given
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with test_dag():
        tasks = builder_factory().create().parse_manifest_into_tasks(manifest_path)

    # then
    run_task = tasks.get_task("model.dbt_test.dim_users").run_airflow_task
    assert run_task.cmds == ["bash", "-c"]
    assert "set -e; dbt --no-write-json run " in run_task.arguments[0]
    assert "--models dim_users" in run_task.arguments[0]
    assert '--vars "{}"' in run_task.arguments[0]
    assert run_task.name == "dim-users-run" if IS_FIRST_AIRFLOW_VERSION else "run"
    assert run_task.task_id == task_group_prefix_builder("dim_users", "run")


def test_test_task():
    # given
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})

    # when
    with test_dag():
        tasks = builder_factory().create().parse_manifest_into_tasks(manifest_path)

    # then
    test_task = tasks.get_task("model.dbt_test.dim_users").test_airflow_task
    assert test_task.cmds == ["bash", "-c"]
    assert "set -e; dbt --no-write-json test " in test_task.arguments[0]
    assert "--models dim_users" in test_task.arguments[0]
    assert '--vars "{}"' in test_task.arguments[0]
    assert test_task.name == "dim-users-test" if IS_FIRST_AIRFLOW_VERSION else "test"
    assert test_task.task_id == task_group_prefix_builder("dim_users", "test")


def test_dbt_vars():
    # given
    manifest_path = manifest_file_with_models({"model.dbt_test.dim_users": []})
    factory = DbtAirflowTasksBuilderFactory(
        path.dirname(path.abspath(__file__)), "vars"
    )

    # when
    with test_dag():
        tasks = factory.create().parse_manifest_into_tasks(manifest_path)

    # then
    run_task = tasks.get_task("model.dbt_test.dim_users").run_airflow_task
    assert run_task.cmds == ["bash", "-c"]
    assert "set -e; dbt --no-write-json run " in run_task.arguments[0]
    assert '--vars "{variable_1: 123, variable_2: var2}"' in run_task.arguments[0]
