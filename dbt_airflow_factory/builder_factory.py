"""Factory creating Airflow tasks."""

from typing import Any, List

import airflow

from dbt_airflow_factory.builder import DbtAirflowTasksBuilder
from dbt_airflow_factory.config_utils import read_config
from dbt_airflow_factory.dbt_parameters import DbtExecutionEnvironmentParameters
from dbt_airflow_factory.k8s_parameters import KubernetesExecutionParameters
from dbt_airflow_factory.operator import KubernetesPodOperatorBuilder


class DbtAirflowTasksBuilderFactory:
    """
    Factory creating Airflow tasks.

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
    """

    base_config_name: str
    """Name of the ``base`` environment (default: ``base``)."""
    dag_path: str
    """path to ``manifest.json`` file."""
    env: str
    """name of the environment."""
    dbt_config_file_name: str
    """name of the DBT config file (default: ``dbt.yml``)."""
    k8s_config_file_name: str
    """name of the Kubernetes config file (default: ``k8s.yml``)."""

    def __init__(
        self,
        dag_path: str,
        env: str,
        dbt_config_file_name: str = "dbt.yml",
        k8s_config_file_name: str = "k8s.yml",
    ):
        self.base_config_name = "base"
        self.dag_path = dag_path
        self.env = env
        self.dbt_config_file_name = dbt_config_file_name
        self.k8s_config_file_name = k8s_config_file_name

    def create(self) -> DbtAirflowTasksBuilder:
        """
        Create :class:`.DbtAirflowTasksBuilder` to use.

        :return: Instance of :class:`.DbtAirflowTasksBuilder`.
        :rtype: DbtAirflowTasksBuilder
        """
        dbt_execution_env_params = self._create_dbt_config()
        kubernetes_params = self._create_k8s_config()
        return DbtAirflowTasksBuilder(
            KubernetesPodOperatorBuilder(dbt_execution_env_params, kubernetes_params)
        )

    def _create_dbt_config(self) -> DbtExecutionEnvironmentParameters:
        return DbtExecutionEnvironmentParameters(
            **read_config(self.dag_path, self.env, self.dbt_config_file_name)
        )

    def _create_k8s_config(self) -> KubernetesExecutionParameters:
        config = read_config(self.dag_path, self.env, self.k8s_config_file_name)
        config["image"] = self._prepare_image(config["image"])
        config["secrets"] = self._prepare_secrets(config)
        config.update(config.pop("resources"))
        return KubernetesExecutionParameters(**config)

    @staticmethod
    def _prepare_image(config: dict) -> str:
        return config["repository"] + ":" + str(config["tag"])

    def _prepare_secrets(self, config: dict) -> List[Any]:
        return [self._prepare_secret(secret) for secret in config["secrets"]]

    @staticmethod
    def _prepare_secret(secret_dict: dict):  # type: ignore
        if airflow.__version__.startswith("1."):
            from airflow.contrib.kubernetes.secret import Secret

            return Secret(**secret_dict)
        else:
            from airflow.kubernetes.secret import Secret

            return Secret(**secret_dict)
