from .utils import *


def test_run_test_dependency():
    #given
    builder = task_builder()
    manifest_path = manifest_file_with_models({"model.dbt_test.model1": []})

    #when
    with TEST_DAG:
        tasks = builder.parse_manifest_into_tasks(manifest_path)

    #then
    assert "model1_test" in tasks.get_task('model.dbt_test.model1').run_airflow_task.downstream_task_ids
    assert "model1_run" in tasks.get_task('model.dbt_test.model1').test_airflow_task.upstream_task_ids


def test_dependency():
    #given
    builder = task_builder()
    manifest_path = manifest_file_with_models({"model.dbt_test.model1": [],
                                               "model.dbt_test.model2": ["model.dbt_test.model1"]})

    #when
    with TEST_DAG:
        tasks = builder.parse_manifest_into_tasks(manifest_path)

    #then
    assert tasks.length() == 2
    assert "model1_test" in tasks.get_task('model.dbt_test.model2').run_airflow_task.upstream_task_ids
    assert "model2_run" in tasks.get_task('model.dbt_test.model1').test_airflow_task.downstream_task_ids


def test_more_complex_dependencies():
    #given
    builder = task_builder()
    manifest_path = manifest_file_with_models(
        {"model.dbt_test.model1": [],
         "model.dbt_test.model2": ["model.dbt_test.model1"],
         "model.dbt_test.model3": ["model.dbt_test.model1", "model.dbt_test.model2"],
         "model.dbt_test.model4": ["model.dbt_test.model3"]})

    #when
    with TEST_DAG:
        tasks = builder.parse_manifest_into_tasks(manifest_path)

    #then
    assert tasks.length() == 4
    assert "model1_test" in tasks.get_task('model.dbt_test.model2').run_airflow_task.upstream_task_ids
    assert "model1_test" in tasks.get_task('model.dbt_test.model3').run_airflow_task.upstream_task_ids
    assert "model2_run" in tasks.get_task('model.dbt_test.model1').test_airflow_task.downstream_task_ids
    assert "model3_run" in tasks.get_task('model.dbt_test.model1').test_airflow_task.downstream_task_ids
    assert "model1_test" in tasks.get_task('model.dbt_test.model3').run_airflow_task.upstream_task_ids
    assert "model2_test" in tasks.get_task('model.dbt_test.model3').run_airflow_task.upstream_task_ids
    assert "model4_run" in tasks.get_task('model.dbt_test.model3').test_airflow_task.downstream_task_ids