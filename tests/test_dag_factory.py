import os
from datetime import datetime, timezone

from dbt_airflow_manifest_parser.airflow_dag_factory import AirflowDagFactory


def test_dag_factory():
    # given
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config")
    factory = AirflowDagFactory(config_path, "dev")

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
    assert len(dag.tasks) == 5
