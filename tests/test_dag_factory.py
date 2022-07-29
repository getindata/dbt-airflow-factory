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


def test_gateway_dag_factory():
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "gateway")

    # when
    dag = factory.create()

    # then save points should be as passed in the config file
    assert factory.airflow_config["save_points"] == ["datalab_stg", "datalab"]


def test_should_not_fail_when_savepoint_property_wasnt_passed():
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "no_gateway")

    # when
    dag = factory.create()

    # then save_points_property should be empty
    assert factory.airflow_config.get("save_points", []).__len__() == 0


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
    gateway_task = [task for task in dag.tasks if task.task_id == f'{save_points[0]}_{save_points[1]}_gateway'][0]

    assert gateway_task.downstream_task_ids == {
        'user.run', 'shop.run', 'payment.run'
    }

    assert gateway_task.upstream_task_ids == {'stg_payment.test', 'stg_shop.test', 'stg_user.test'}

