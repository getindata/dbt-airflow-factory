from .utils import (
    builder_factory,
    manifest_file_with_models,
    task_group_prefix_builder,
    test_dag,
)

extra_metadata_data = {
    "child_map": {
        "source.upstream_pipeline_sources.upstream_pipeline.some_final_model": [
            "model.dbt_test.dependent_model"
        ],
        "source.upstream_pipeline_sources.upstream_pipeline.unused": [],
    },
    "sources": {
        "source.upstream_pipeline_sources.upstream_pipeline.some_final_model": {
            "database": "gid-dataops-labs",
            "schema": "presentation",
            "name": "some_final_model",
            "unique_id": "source.upstream_pipeline_sources.upstream_pipeline.some_final_model",
            "source_meta": {"dag": "dbt-tpch-test"},
        },
        "source.upstream_pipeline_sources.upstream_pipeline.unused": {
            "database": "gid-dataops-labs",
            "schema": "presentation",
            "name": "unused",
            "unique_id": "source.upstream_pipeline_sources.upstream_pipeline.unused",
            "source_meta": {"dag": "dbt-tpch-test"},
        },
        "source.upstream_pipeline_sources.upstream_pipeline.no_dag": {
            "database": "gid-dataops-labs",
            "schema": "presentation",
            "name": "no_dag",
            "unique_id": "source.upstream_pipeline_sources.upstream_pipeline.no_dag",
            "source_meta": {},
        },
    },
}


def test_dag_sensor():
    # given
    manifest_path = manifest_file_with_models(
        {
            "model.dbt_test.dependent_model": [
                "source.upstream_pipeline_sources.upstream_pipeline.some_final_model"
            ]
        },
        extra_metadata_data,
    )

    # when
    with test_dag():
        tasks = (
            builder_factory(enable_project_dependencies=True)
            .create()
            .parse_manifest_into_tasks(manifest_path)
        )

    # then
    sensor_task = tasks.get_task(
        "source.upstream_pipeline_sources.upstream_pipeline.some_final_model"
    )
    assert tasks.length() == 2
    assert sensor_task is not None
    assert sensor_task.execution_airflow_task is not None
    assert sensor_task.test_airflow_task is None
    assert sensor_task.execution_airflow_task.task_id == "sensor_some_final_model"


def test_dag_sensor_dependency():
    # given
    manifest_path = manifest_file_with_models(
        {
            "model.dbt_test.dependent_model": [
                "source.upstream_pipeline_sources.upstream_pipeline.some_final_model"
            ]
        },
        extra_metadata_data,
    )

    # when
    with test_dag():
        tasks = (
            builder_factory(enable_project_dependencies=True)
            .create()
            .parse_manifest_into_tasks(manifest_path)
        )

    # then
    assert (
        "sensor_some_final_model"
        in tasks.get_task("model.dbt_test.dependent_model").execution_airflow_task.upstream_task_ids
    )
    assert (
        task_group_prefix_builder("dependent_model", "run")
        in tasks.get_task(
            "source.upstream_pipeline_sources.upstream_pipeline.some_final_model"
        ).execution_airflow_task.downstream_task_ids
    )


def test_dag_sensor_no_meta():
    # given
    manifest_path = manifest_file_with_models(
        {
            "model.dbt_test.dependent_model": [
                "source.upstream_pipeline_sources.upstream_pipeline.some_final_model",
                "source.upstream_pipeline_sources.upstream_pipeline.no_dag",
            ]
        },
        extra_metadata_data,
    )

    # when
    with test_dag():
        tasks = (
            builder_factory(enable_project_dependencies=True)
            .create()
            .parse_manifest_into_tasks(manifest_path)
        )

    # then
    assert tasks.length() == 2
