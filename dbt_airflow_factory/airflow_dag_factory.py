"""Factory creating Airflow DAG."""

import os

import airflow
from airflow import DAG
from airflow.models import BaseOperator

if airflow.__version__.startswith("1."):
    from airflow.operators.dummy_operator import DummyOperator
else:
    from airflow.operators.dummy import DummyOperator

from pytimeparse import parse

from dbt_airflow_factory.builder import DbtAirflowTasksBuilder
from dbt_airflow_factory.builder_factory import DbtAirflowTasksBuilderFactory
from dbt_airflow_factory.config_utils import read_config


class AirflowDagFactory:
    """
    Factory creating Airflow DAG.

    :param dag_path: path to ``manifest.json`` file.
    :type dag_path: str
    :param env: name of the environment.
    :type env: str
    :param dbt_config_file_name: name of the DBT config file.
        If not specified, default value is ``dbt.yml``.
    :type dbt_config_file_name: str
    :param k8s_config_file_name: name of the Kubernetes config file.
        If not specified, default value is ``k8s.yml``.
    :type k8s_config_file_name: str
    :param airflow_config_file_name: name of the Airflow config file.
        If not specified, default value is ``airflow.yml``.
    :type airflow_config_file_name: str
    """

    _builder: DbtAirflowTasksBuilder
    dag_path: str
    """path to ``manifest.json`` file."""
    env: str
    """name of the environment."""
    airflow_config_file_name: str
    """name of the Airflow config file (default: ``airflow.yml``)."""

    def __init__(
        self,
        dag_path: str,
        env: str,
        dbt_config_file_name: str = "dbt.yml",
        k8s_config_file_name: str = "k8s.yml",
        airflow_config_file_name: str = "airflow.yml",
    ):
        self._builder = DbtAirflowTasksBuilderFactory(
            dag_path, env, dbt_config_file_name, k8s_config_file_name
        ).create()
        self.dag_path = dag_path
        self.env = env
        self.airflow_config_file_name = airflow_config_file_name

    def create(self) -> DAG:
        """
        Parse ``manifest.json`` and create tasks based on the data contained there.

        :return: Generated DAG.
        :rtype: airflow.models.dag.DAG
        """
        config = self.read_config()
        with DAG(default_args=config["default_args"], **config["dag"]) as dag:
            self.create_tasks(config)
        return dag

    def create_tasks(self, config: dict) -> None:
        """
        Parse ``manifest.json`` and create tasks based on the data contained there.

        :param config: Dictionary representing ``airflow.yml``.
        :type config: dict
        """
        start = self._create_starting_task(config)
        end = DummyOperator(task_id="end")
        tasks = self._builder.parse_manifest_into_tasks(
            self._manifest_file_path(config), config.get("use_task_group") or False
        )
        for starting_task in tasks.get_starting_tasks():
            start >> starting_task.get_start_task()
        for ending_task in tasks.get_ending_tasks():
            ending_task.get_end_task() >> end

    def _create_starting_task(self, config: dict) -> BaseOperator:
        if config.get("seed_task", True):
            return self._builder.create_seed_task()
        else:
            return DummyOperator(task_id="start")

    def _manifest_file_path(self, config: dict) -> str:
        file_dir = config.get("manifest_dir_path", self.dag_path)
        return os.path.join(file_dir, config.get("manifest_file_name", "manifest.json"))

    def read_config(self) -> dict:
        """
        Read ``airflow.yml`` from ``config`` directory into a dictionary.

        :return: Dictionary representing ``airflow.yml``.
        :rtype: dict
        :raises KeyError: No ``default_args`` key in ``airflow.yml``.
        """
        config = read_config(
            self.dag_path, self.env, self.airflow_config_file_name, replace_jinja=True
        )
        if "retry_delay" in config["default_args"]:
            config["default_args"]["retry_delay"] = parse(
                config["default_args"]["retry_delay"]
            )
        return config
