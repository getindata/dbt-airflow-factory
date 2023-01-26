"""POD representing Kubernetes operator config file."""

from typing import Any, List

import airflow

from dbt_airflow_factory.config_utils import read_config
from dbt_airflow_factory.k8s.k8s_parameters import KubernetesExecutionParameters


class KubernetesExecutionParametersLoader:
    @staticmethod
    def create_config(
        dag_path: str, env: str, execution_env_config_file_name: str
    ) -> KubernetesExecutionParameters:
        config = read_config(dag_path, env, "k8s.yml")
        config = KubernetesExecutionParametersLoader._update_config_if_datahub_exits(
            config, read_config(dag_path, env, "datahub.yml")
        )
        config.update(read_config(dag_path, env, execution_env_config_file_name))
        config["image"] = KubernetesExecutionParametersLoader._prepare_image(config["image"])
        config["secrets"] = KubernetesExecutionParametersLoader._prepare_secrets(config)
        config.update(config.pop("resources"))
        return KubernetesExecutionParameters(**config)

    @staticmethod
    def _prepare_image(config: dict) -> str:
        return config["repository"] + ":" + str(config["tag"])

    @staticmethod
    def _prepare_secrets(config: dict) -> List[Any]:
        return [
            KubernetesExecutionParametersLoader._prepare_secret(secret)
            for secret in config["secrets"]
        ]

    @staticmethod
    def _prepare_secret(secret_dict: dict):  # type: ignore
        if airflow.__version__.startswith("1."):
            from airflow.contrib.kubernetes.secret import Secret

            return Secret(**secret_dict)
        else:
            from airflow.kubernetes.secret import Secret

            return Secret(**secret_dict)

    @staticmethod
    def _update_config_if_datahub_exits(config: dict, datahub_config: dict) -> dict:
        if datahub_config:
            config["envs"].update({"DATAHUB_GMS_URL": datahub_config["sink"]["config"]["server"]})
        return config
