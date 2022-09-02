"""Factory creating Airflow DAG."""

import os

import airflow
from airflow import DAG
from airflow.models import BaseOperator

from dbt_airflow_factory.ingestion import IngestionEngine, IngestionFactory

if airflow.__version__.startswith("1."):
    from airflow.operators.dummy_operator import DummyOperator
else:
    from airflow.operators.dummy import DummyOperator

from pytimeparse import parse

from dbt_airflow_factory.builder_factory import DbtAirflowTasksBuilderFactory
from dbt_airflow_factory.config_utils import read_config, read_env_config
from dbt_airflow_factory.notifications.handler import NotificationHandlersFactory
from dbt_airflow_factory.tasks_builder.builder import DbtAirflowTasksBuilder


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
    :param execution_env_config_file_name: name of the execution env config file.
        If not specified, default value is ``execution_env.yml``.
    :type execution_env_config_file_name: str
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
        execution_env_config_file_name: str = "execution_env.yml",
        airflow_config_file_name: str = "airflow.yml",
        airbyte_config_file_name: str = "airbyte.yml",
        ingestion_config_file_name: str = "ingestion.yml",
    ):
        self._notifications_handlers_builder = NotificationHandlersFactory()
        self.airflow_config = self._read_config(dag_path, env, airflow_config_file_name)
        self._builder = DbtAirflowTasksBuilderFactory(
            dag_path,
            env,
            self.airflow_config,
            dbt_config_file_name,
            execution_env_config_file_name,
        ).create()
        self.dag_path = dag_path
        airbyte_config = read_env_config(
            dag_path=dag_path, env=env, file_name=airbyte_config_file_name
        )
        self.ingestion_config = read_env_config(
            dag_path=dag_path, env=env, file_name=ingestion_config_file_name
        )
        self.ingestion_tasks_builder_factory = IngestionFactory(
            ingestion_config=airbyte_config,
            name=IngestionEngine.value_of(
                self.ingestion_config.get("engine", IngestionEngine.AIRBYTE.value)
            ),
        )

    def create(self) -> DAG:
        """
        Parse ``manifest.json`` and create tasks based on the data contained there.

        :return: Generated DAG.
        :rtype: airflow.models.dag.DAG
        """
        with DAG(
            default_args=self.airflow_config["default_args"], **self.airflow_config["dag"]
        ) as dag:
            self.create_tasks()
        return dag

    def create_tasks(self) -> None:
        """
        Parse ``manifest.json`` and create tasks based on the data contained there.
        """

        ingestion_enabled = self.ingestion_config.get("enable", False)

        start = self._create_starting_task()
        if ingestion_enabled and self.ingestion_tasks_builder_factory:
            builder = self.ingestion_tasks_builder_factory.create()
            ingestion_tasks = builder.build()
            ingestion_tasks >> start
        end = DummyOperator(task_id="end")
        tasks = self._builder.parse_manifest_into_tasks(self._manifest_file_path())
        for starting_task in tasks.get_starting_tasks():
            start >> starting_task.get_start_task()
        for ending_task in tasks.get_ending_tasks():
            ending_task.get_end_task() >> end

    def _create_starting_task(self) -> BaseOperator:
        if self.airflow_config.get("seed_task", True):
            return self._builder.create_seed_task()
        else:
            return DummyOperator(task_id="start")

    def _manifest_file_path(self) -> str:
        file_dir = self.airflow_config.get("manifest_dir_path", self.dag_path)
        return os.path.join(
            file_dir, self.airflow_config.get("manifest_file_name", "manifest.json")
        )

    def _read_config(self, dag_path: str, env: str, airflow_config_file_name: str) -> dict:
        """
        Read ``airflow.yml`` from ``config`` directory into a dictionary.

        :return: Dictionary representing ``airflow.yml``.
        :rtype: dict
        :raises KeyError: No ``default_args`` key in ``airflow.yml``.
        """
        config = read_config(dag_path, env, airflow_config_file_name, replace_jinja=True)
        if "retry_delay" in config["default_args"]:
            config["default_args"]["retry_delay"] = parse(config["default_args"]["retry_delay"])
        if "failure_handlers" in config:
            config["default_args"][
                "on_failure_callback"
            ] = self._notifications_handlers_builder.create_failure_handler(
                config["failure_handlers"]
            )
        return config
