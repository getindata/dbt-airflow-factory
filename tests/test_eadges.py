from .utils import manifest_file_with_models, builder_factory, test_dag


def test_starting_tasks():
    # given
    manifest_path = manifest_file_with_models(
        {
            "model.dbt_test.model1": [],
            "model.dbt_test.model2": [],
            "model.dbt_test.model3": ["model.dbt_test.model1", "model.dbt_test.model2"],
            "model.dbt_test.model4": ["model.dbt_test.model3"],
            "model.dbt_test.model5": [],
        }
    )

    # when
    with test_dag():
        tasks = builder_factory().create().parse_manifest_into_tasks(manifest_path)

    # then
    starting_tasks_names = [
        task.run_airflow_task.task_id for task in tasks.get_starting_tasks()
    ]
    assert "model1_run" in starting_tasks_names
    assert "model2_run" in starting_tasks_names
    assert "model5_run" in starting_tasks_names


def test_ending_tasks():
    # given
    manifest_path = manifest_file_with_models(
        {
            "model.dbt_test.model1": [],
            "model.dbt_test.model2": [],
            "model.dbt_test.model3": ["model.dbt_test.model1", "model.dbt_test.model2"],
            "model.dbt_test.model4": ["model.dbt_test.model3"],
            "model.dbt_test.model5": [],
        }
    )

    # when
    with test_dag():
        tasks = builder_factory().create().parse_manifest_into_tasks(manifest_path)

    # then
    ending_tasks_names = [
        task.test_airflow_task.task_id for task in tasks.get_ending_tasks()
    ]
    assert "model4_test" in ending_tasks_names
    assert "model5_test" in ending_tasks_names
