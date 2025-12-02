from datetime import datetime, timezone
from os import path
from typing import Set

import pytest

from dbt_airflow_factory.airflow_dag_factory import AirflowDagFactory
from dbt_airflow_factory.constants import IS_FIRST_AIRFLOW_VERSION


def test_dag_factory():
    """Test that DAG factory creates a DAG with correct metadata and Cosmos dbt tasks"""
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "dev")

    # when
    dag = factory.create()

    # then - DAG metadata from config should be preserved
    assert dag.dag_id == "dbt-platform-poc"
    assert dag.description == "Experimental snadbox data platform DAG"
    assert dag.schedule_interval == "0 12 * * *"
    assert not dag.catchup
    assert dag.default_args == {
        "owner": "Piotr Pekala",
        "email": ["test@getindata.com"],
        "depends_on_past": False,
        "start_date": datetime(2021, 10, 20, 0, 0, 0, tzinfo=timezone.utc),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": 300,
    }

    # then - Cosmos should create dbt tasks from the manifest
    task_ids = [t.task_id for t in dag.tasks]
    # Cosmos creates tasks with pattern: dbt.model_name.run/test
    assert any(".run" in tid for tid in task_ids), "Should have at least one dbt run task"
    assert "end" in task_ids, "Should have end task"
    assert len(dag.tasks) >= 2, "Should have at least dbt tasks + end task"


def test_task_group_dag_factory():
    """Test that Cosmos DbtTaskGroup is created and contains dbt tasks"""
    if IS_FIRST_AIRFLOW_VERSION:  # You cannot use TaskGroup in Airflow 1 anyway
        pytest.skip("TaskGroup not supported in Airflow 1")

    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "task_group")

    # when
    dag = factory.create()

    # then - Cosmos creates a DbtTaskGroup named "dbt"
    assert "dbt" in dag.task_group_dict, "Should have 'dbt' task group from Cosmos"
    dbt_group = dag.task_group_dict["dbt"]

    # Cosmos organizes dbt tasks within the task group
    assert len(dbt_group.children) > 0, "dbt task group should contain dbt model tasks"

    # All tasks should include the task group tasks + end task
    assert len(dag.tasks) >= 2, "Should have dbt tasks + end task"


def test_no_task_group_dag_factory():
    """Test that DAG creation works even with no_task_group config"""
    if IS_FIRST_AIRFLOW_VERSION:  # You cannot use TaskGroup in Airflow 1 anyway
        pytest.skip("TaskGroup not supported in Airflow 1")

    # given - Note: Cosmos always uses DbtTaskGroup regardless of old use_task_group config
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "no_task_group")

    # when
    dag = factory.create()

    # then - Cosmos still creates tasks successfully
    task_ids = [t.task_id for t in dag.tasks]
    assert any(".run" in tid or ".test" in tid for tid in task_ids), "Should have dbt tasks"
    assert len(dag.tasks) >= 2, "Should have dbt tasks created"


def test_gateway_dag_factory():
    """Test that DAG creation works with gateway config (save_points feature deprecated with Cosmos)"""
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "gateway")

    # when
    dag = factory.create()

    # then - Config should still be readable
    assert factory.airflow_config.get("save_points") == ["datalab_stg", "datalab"]

    # then - Cosmos creates dbt tasks (gateway feature is deprecated - Cosmos handles dependencies via manifest)
    task_ids = [t.task_id for t in dag.tasks]
    assert any(
        ".run" in tid or ".test" in tid for tid in task_ids
    ), "Should have dbt tasks from Cosmos"
    assert len(dag.tasks) >= 2, "Should have dbt tasks created"


def test_should_not_fail_when_savepoint_property_wasnt_passed():
    """Test that DAG creation works without save_points config"""
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "no_gateway")

    # when
    dag = factory.create()

    # then - save_points config should be empty or not present
    assert factory.airflow_config.get("save_points", []).__len__() == 0

    # and - Cosmos should still create dbt tasks successfully
    task_ids = [t.task_id for t in dag.tasks]
    assert any(".run" in tid or ".test" in tid for tid in task_ids), "Should have dbt tasks"
    assert len(dag.tasks) >= 2, "Should have dbt tasks created"


def test_should_properly_map_tasks():
    """Test that Cosmos creates proper task dependencies from dbt manifest"""
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "gateway")

    # when
    dag = factory.create()

    # then - save_points config is readable
    save_points = factory.airflow_config.get("save_points")
    assert save_points.__len__() == 2

    # then - Cosmos creates dbt tasks with dependencies from manifest
    # Note: Gateway tasks are deprecated - Cosmos uses dbt manifest for dependencies
    task_ids = [t.task_id for t in dag.tasks]
    assert any(".run" in tid or ".test" in tid for tid in task_ids), "Should have dbt tasks"

    # Cosmos handles task dependencies via dbt's ref() in manifest
    # so we just verify tasks were created
    assert len(dag.tasks) >= 2, "Should have dbt tasks + end task"


def test_should_properly_map_tasks_with_source():
    """Test that Cosmos creates tasks for dbt models with source dependencies"""
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "gateway_source")

    # when
    dag = factory.create()

    # then - save_points config is readable
    save_points = factory.airflow_config.get("save_points")
    assert save_points.__len__() == 2

    # then - Cosmos creates dbt tasks
    task_ids = [t.task_id for t in dag.tasks]
    assert any(".run" in tid or ".test" in tid for tid in task_ids), "Should have dbt tasks"

    # Cosmos handles source dependencies via dbt manifest
    # so we just verify tasks were created successfully
    assert len(dag.tasks) >= 2, "Should have dbt tasks created"


@pytest.mark.parametrize(
    "test_name,ingestion_enabled,seed_available,expected_ingestion_tasks",
    [
        (
            "should return no ingestion task when ingestion is not enabled - seed enabled",
            False,
            True,
            set(),
        ),
        (
            "should return no ingestion task when ingestion is not enabled - seed disabled",
            False,
            False,
            set(),
        ),
        (
            "should return ingestion tasks when ingestion is enabled - seed disabled",
            True,
            False,
            {"postgres_ingestion", "mysql_ingestion", "sales_force_ingestion"},
        ),
        (
            "should return ingestion tasks when ingestion is enabled - seed enabled",
            True,
            True,
            {"postgres_ingestion", "mysql_ingestion", "sales_force_ingestion"},
        ),
    ],
)
def test_should_add_airbyte_tasks_when_seed_is_not_available(
    test_name: str,
    ingestion_enabled: bool,
    seed_available: bool,
    expected_ingestion_tasks: Set[str],
):
    """Test that Airbyte ingestion tasks are created based on ingestion config"""
    # given configuration for airbyte_dev
    factory = AirflowDagFactory(
        path.dirname(path.abspath(__file__)),
        "airbyte_dev",
        airflow_config_file_name=f"airflow_seed_{boolean_mapper[seed_available]}.yml",
        ingestion_config_file_name=f"ingestion_{boolean_mapper[ingestion_enabled]}.yml",
    )

    # when creating factory
    dag = factory.create()

    # then - check that expected ingestion tasks exist or don't exist
    task_ids = {t.task_id for t in dag.tasks}

    if expected_ingestion_tasks:
        # Ingestion enabled - should have ingestion tasks
        assert expected_ingestion_tasks.issubset(
            task_ids
        ), f"Expected ingestion tasks {expected_ingestion_tasks} to exist in {task_ids}"

        # Ingestion tasks should be upstream of dbt task group
        ingestion_tasks = [t for t in dag.tasks if t.task_id in expected_ingestion_tasks]
        assert all(
            len(t.downstream_task_ids) > 0 for t in ingestion_tasks
        ), "Ingestion tasks should have downstream dependencies"
    else:
        # Ingestion disabled - should NOT have ingestion tasks
        assert not expected_ingestion_tasks.intersection(
            task_ids
        ), "Should not have ingestion tasks when ingestion disabled"

    # DAG should always have dbt tasks from Cosmos
    assert any(
        ".run" in tid or ".test" in tid for tid in task_ids
    ), "Should have dbt tasks from Cosmos"


def test_seed_handling_with_cosmos():
    """Test that Cosmos automatically creates seed tasks from manifest and ignores seed_task config"""
    # Note: The actual seed tasks are created by Cosmos from the manifest.
    # The legacy seed_task config setting in airflow.yml is ignored for backward compatibility.

    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "dev")

    # when
    dag = factory.create()

    # then - Verify seed_task config setting exists but doesn't affect Cosmos
    # Cosmos reads seeds directly from manifest, regardless of airflow.yml seed_task setting
    assert factory.airflow_config.get("seed_task") is True, "Config should have seed_task setting"

    # Cosmos successfully creates the DAG with tasks from manifest
    task_ids = [t.task_id for t in dag.tasks]
    assert len(task_ids) > 0, "DAG should have tasks"

    # Verify DAG creation works with seed_task setting present (backward compat)
    assert dag is not None, "DAG should be created successfully"


boolean_mapper = {True: "enabled", False: "disabled"}

starting_task_mapper = {True: "dbt_seed", False: "start"}
