from datetime import datetime, timezone
from os import path
from typing import List, Set

import pytest

from dbt_airflow_factory.airflow_dag_factory import AirflowDagFactory

from .utils import IS_FIRST_AIRFLOW_VERSION


def test_dag_factory():
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "dev")

    # when
    dag = factory.create()

    # then
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
    assert len(dag.tasks) == 4


def test_task_group_dag_factory():
    if IS_FIRST_AIRFLOW_VERSION:  # You cannot use TaskGroup in Airflow 1 anyway
        return True

    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "task_group")

    # when
    dag = factory.create()

    # then
    assert len(dag.tasks) == 10
    assert len(dag.task_group.children) == 6


def test_no_task_group_dag_factory():
    if IS_FIRST_AIRFLOW_VERSION:  # You cannot use TaskGroup in Airflow 1 anyway
        return True

    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "no_task_group")

    # when
    dag = factory.create()

    # then
    assert len(dag.tasks) == 10
    assert len(dag.task_group.children) == 10


@pytest.mark.parametrize(
    "test_name,ingestion_enabled,seed_available,expected_start_task_deps",
    [
            (
                    "should return no ingestion task when ingestion is not enabled - seed enabled",
                    False,
                    True,
                    {"postgres_ingestion", "mysql_ingestion", "sales_force_ingestion"}
            ),
            (
                    "should return no ingestion task when ingestion is not enabled - seed disabled",
                    False,
                    False,
                    set()
            ),
            (
                    "should return ingestion tasks when ingestion is enabled - seed disabled",
                    True,
                    False,
                    {"postgres_ingestion", "mysql_ingestion", "sales_force_ingestion"}
            ),
            (
                    "should return ingestion tasks when ingestion is enabled - seed enabled",
                    True,
                    True,
                    set()
            ),
    ]
)
def test_should_add_airbyte_tasks_when_seed_is_not_available(test_name: str, ingestion_enabled: bool,
                                                             seed_available: bool,
                                                             expected_start_task_deps: Set[str]):
    # given configuration for airbyte_dev
    factory = AirflowDagFactory(
        path.dirname(path.abspath(__file__)), "airbyte_dev",
        airflow_config_file_name=f"airflow_seed_{boolean_mapper[seed_available]}.yml",
        ingestion_config_file_name=f"ingestion_{boolean_mapper[ingestion_enabled]}.yml"
    )

    # when creating factory
    dag = factory.create()

    # airbyte ingestion tasks should be added to dummy task
    start_task_name = [
        task for task in dag.tasks if task.task_id == starting_task_mapper[seed_available]
    ][0]

    assert start_task_name.upstream_task_ids == expected_start_task_deps


boolean_mapper = {
    True: "enabled",
    False: "disabled"
}

starting_task_mapper = {
    True: "dbt_seed",
    False: "start"
}