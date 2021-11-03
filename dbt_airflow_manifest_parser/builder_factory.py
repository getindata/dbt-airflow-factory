import airflow

from dbt_airflow_manifest_parser.builder import DbtAirflowTasksBuilder
from dbt_airflow_manifest_parser.config_utils import read_config
from dbt_airflow_manifest_parser.dbt_parameters import DbtExecutionEnvironmentParameters
from dbt_airflow_manifest_parser.k8s_parameters import KubernetesExecutionParameters
from dbt_airflow_manifest_parser.operator import KubernetesPodOperatorBuilder


class DbtAirflowTasksBuilderFactory:
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
        dbt_execution_env_params = self._create_dbt_config()
        kubernetes_params = self._create_k8s_config()
        return DbtAirflowTasksBuilder(
            KubernetesPodOperatorBuilder(dbt_execution_env_params, kubernetes_params)
        )

    def _create_dbt_config(self):
        return DbtExecutionEnvironmentParameters(
            **read_config(self.dag_path, self.env, self.dbt_config_file_name)
        )

    def _create_k8s_config(self):
        config = read_config(self.dag_path, self.env, self.k8s_config_file_name)
        config["image"] = self._prepare_image(config["image"])
        config["secrets"] = self._prepare_secrets(config)
        config.update(config.pop("resources"))
        return KubernetesExecutionParameters(**config)

    @staticmethod
    def _prepare_image(config):
        return config["repository"] + ":" + str(config["tag"])

    def _prepare_secrets(self, config):
        return [self._prepare_secret(secret) for secret in config["secrets"]]

    @staticmethod
    def _prepare_secret(secret_dict):
        if airflow.__version__.startswith("1."):
            from airflow.contrib.kubernetes.secret import Secret

            return Secret(**secret_dict)
        else:
            from airflow.kubernetes.secret import Secret

            return Secret(**secret_dict)
