from os import path
from unittest import mock

from dbt_airflow_factory.airflow_dag_factory import AirflowDagFactory

from .utils import IS_FIRST_AIRFLOW_VERSION


@mock.patch("airflow.models.variable.Variable")
def test_airflow_vars_parsing(variable_mock):
    vars_dict = {
        "dags_path": "gs://some-interesting-bucket/dags/path",
        "dags_owner": "Some Guy",
        "email_owner": "dags.owner@example.com",
    }

    variable_mock.configure_mock(**{"get_variable_from_secrets": lambda key: vars_dict.get(key)})

    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "airflow_vars")

    # when
    if IS_FIRST_AIRFLOW_VERSION:
        with mock.patch("airflow.models.variable.get_variable", lambda key: vars_dict.get(key)):
            config = factory.read_config()
    else:
        config = factory.read_config()

    # then
    assert config["dags_path"] == vars_dict["dags_path"]
    assert config["default_args"]["owner"] == vars_dict["dags_owner"]
    assert config["default_args"]["email"] == [vars_dict["email_owner"]]
