from datetime import datetime, timezone
from os import path

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
