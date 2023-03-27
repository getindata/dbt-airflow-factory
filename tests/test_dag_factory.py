from datetime import datetime, timezone
from os import path
from typing import Set

import pytest

from dbt_airflow_factory.airflow_dag_factory import AirflowDagFactory
from dbt_airflow_factory.constants import IS_FIRST_AIRFLOW_VERSION


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


def test_gateway_dag_factory():
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "gateway")

    # when
    dag = factory.create()

    # then save points should be as passed in the config file
    assert dag.tasks.__len__() == 15
    assert factory.airflow_config["save_points"] == ["datalab_stg", "datalab"]


def test_should_not_fail_when_savepoint_property_wasnt_passed():
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "no_gateway")

    # when
    dag = factory.create()

    # then save_points_property should be empty
    assert factory.airflow_config.get("save_points", []).__len__() == 0

    # and number of tasks should match
    assert dag.tasks.__len__() == 4


def test_should_properly_map_tasks():
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "gateway")

    # when
    dag = factory.create()

    # then save_points_property should be empty
    save_points = factory.airflow_config.get("save_points")
    assert save_points.__len__() == 2

    # and number of tasks should be as expected
    assert dag.tasks.__len__() == 15

    # and tasks should be correctly matched to themselves
    gateway_task = [
        task for task in dag.tasks if task.task_id == f"{save_points[0]}_{save_points[1]}_gateway"
    ][0]

    assert gateway_task.downstream_task_ids == {"user.run", "shop.run", "payment.run"}

    assert gateway_task.upstream_task_ids == {"stg_payment.test", "stg_shop.test", "stg_user.test"}


def test_should_properly_map_tasks_with_source():
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "gateway_source")

    # when
    dag = factory.create()

    # then save_points_property should be empty
    save_points = factory.airflow_config.get("save_points")
    assert save_points.__len__() == 2

    # and number of tasks should be as expected
    assert dag.tasks.__len__() == 7

    # and tasks should be correctly matched to themselves
    gateway_task = [
        task for task in dag.tasks if task.task_id == f"{save_points[0]}_{save_points[1]}_gateway"
    ][0]

    assert gateway_task.downstream_task_ids == {"my_second_dbt_model.run"}

    assert gateway_task.upstream_task_ids == {"my_first_dbt_model.test"}


@pytest.mark.parametrize(
    "test_name,ingestion_enabled,seed_available,expected_start_task_deps",
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
    expected_start_task_deps: Set[str],
):
    # given configuration for airbyte_dev
    factory = AirflowDagFactory(
        path.dirname(path.abspath(__file__)),
        "airbyte_dev",
        airflow_config_file_name=f"airflow_seed_{boolean_mapper[seed_available]}.yml",
        ingestion_config_file_name=f"ingestion_{boolean_mapper[ingestion_enabled]}.yml",
    )

    # when creating factory
    dag = factory.create()

    # airbyte ingestion tasks should be added to dummy task
    start_task_name = [
        task for task in dag.tasks if task.task_id == starting_task_mapper[seed_available]
    ][0]

    assert start_task_name.upstream_task_ids == expected_start_task_deps


boolean_mapper = {True: "enabled", False: "disabled"}

starting_task_mapper = {True: "dbt_seed", False: "start"}
