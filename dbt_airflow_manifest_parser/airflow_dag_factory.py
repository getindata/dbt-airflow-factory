import os

import airflow
from airflow import DAG

if airflow.__version__.startswith("1."):
    from airflow.operators.dummy_operator import DummyOperator
else:
    from airflow.operators.dummy import DummyOperator

from pytimeparse import parse

from dbt_airflow_manifest_parser.builder_factory import DbtAirflowTasksBuilderFactory
from dbt_airflow_manifest_parser.config_utils import read_config


class AirflowDagFactory:
    def __init__(
        self,
        dag_path,
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
        config = self.read_config()
        with DAG(default_args=config["default_args"], **config["dag"]) as dag:
            self.create_tasks(config)
        return dag

    def create_tasks(self, config):
        start = self._create_starting_task(config)
        end = DummyOperator(task_id="end")
        tasks = self._builder.parse_manifest_into_tasks(
            self._manifest_file_path(config), config.get("use_task_group") or False
        )
        for starting_task in tasks.get_starting_tasks():
            start >> starting_task.run_airflow_task
        for ending_task in tasks.get_ending_tasks():
            ending_task.test_airflow_task >> end

    def _create_starting_task(self, config):
        if config.get("seed_task", True):
            return self._builder.create_seed_task()
        else:
            return DummyOperator(task_id="start")

    def _manifest_file_path(self, config: dict) -> str:
        file_dir = config.get("manifest_dir_path", self.dag_path)
        return os.path.join(file_dir, config.get("manifest_file_name", "manifest.json"))

    def read_config(self) -> dict:
        config = read_config(self.dag_path, self.env, self.airflow_config_file_name)
        if "retry_delay" in config["default_args"]:
            config["default_args"]["retry_delay"] = parse(
                config["default_args"]["retry_delay"]
            )
        return config
