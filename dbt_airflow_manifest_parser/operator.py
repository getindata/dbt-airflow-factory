import abc

import airflow

from dbt_airflow_manifest_parser.parameters import (
    DbtExecutionEnvironmentParameters,
    KubernetesExecutionParameters,
)

if airflow.__version__.startswith("1."):
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
else:
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
        KubernetesPodOperator,
    )

from airflow.models.baseoperator import BaseOperator


class DbtRunOperatorBuilder(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create(self, name, model, command) -> BaseOperator:
        raise NotImplementedError


class KubernetesPodOperatorBuilder(DbtRunOperatorBuilder):
    def __init__(
        self,
        dbt_execution_env_parameters: DbtExecutionEnvironmentParameters,
        kubernetes_execution_parameters: KubernetesExecutionParameters,
    ):
        self.dbt_execution_env_parameters = dbt_execution_env_parameters
        self.kubernetes_execution_parameters = kubernetes_execution_parameters

    def create(self, name: str, model: str, command: str) -> BaseOperator:
        return KubernetesPodOperator(
            namespace=self.kubernetes_execution_parameters.namespace,
            image=self.kubernetes_execution_parameters.image,
            cmds=["bash", "-c"],
            node_selectors=self.kubernetes_execution_parameters.node_selectors,
            tolerations=self.kubernetes_execution_parameters.tolerations,
            annotations=self.kubernetes_execution_parameters.annotations,
            arguments=[
                f"set -e; "
                f"dbt --no-write-json {command} "
                f"--target {self.dbt_execution_env_parameters.target} "
                f"--models {model} "
                f"--project-dir {self.dbt_execution_env_parameters.project_dir_path} "
                f"--profiles-dir {self.dbt_execution_env_parameters.profile_dir_path}"
            ],
            labels=self.kubernetes_execution_parameters.labels,
            name=name,
            task_id=name,
            resources=self.kubernetes_execution_parameters.get_resources(),
            secrets=self.kubernetes_execution_parameters.secrets,
            is_delete_operator_pod=self.kubernetes_execution_parameters.is_delete_operator_pod,
            hostnetwork=False,
        )
