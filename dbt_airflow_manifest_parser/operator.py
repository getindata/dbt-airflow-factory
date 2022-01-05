import abc

import airflow

from dbt_airflow_manifest_parser.dbt_parameters import DbtExecutionEnvironmentParameters
from dbt_airflow_manifest_parser.k8s_parameters import KubernetesExecutionParameters

if airflow.__version__.startswith("1."):
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
else:
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
        KubernetesPodOperator,
    )

from airflow.models.baseoperator import BaseOperator


class DbtRunOperatorBuilder(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create(self, name: str, command: str, model: str = None) -> BaseOperator:
        raise NotImplementedError


class KubernetesPodOperatorBuilder(DbtRunOperatorBuilder):
    def __init__(
        self,
        dbt_execution_env_parameters: DbtExecutionEnvironmentParameters,
        kubernetes_execution_parameters: KubernetesExecutionParameters,
    ):
        self.dbt_execution_env_parameters = dbt_execution_env_parameters
        self.kubernetes_execution_parameters = kubernetes_execution_parameters

    def create(self, name: str, command: str, model: str = None) -> BaseOperator:
        return self._create(self._prepare_arguments(command, model), name)

    def _prepare_arguments(self, command: str, model: str):
        args = (
            f"set -e; "
            f"dbt --no-write-json {command} "
            f"--target {self.dbt_execution_env_parameters.target} "
            f'--vars "{self.dbt_execution_env_parameters.vars}" '
        )
        if model:
            args += f"--models {model} "
        args += (
            f"--project-dir {self.dbt_execution_env_parameters.project_dir_path} "
            f"--profiles-dir {self.dbt_execution_env_parameters.profile_dir_path}"
        )
        return [args]

    def _create(self, args, name):
        return KubernetesPodOperator(
            namespace=self.kubernetes_execution_parameters.namespace,
            image=self.kubernetes_execution_parameters.image,
            image_pull_policy=self.kubernetes_execution_parameters.image_pull_policy,
            cmds=["bash", "-c"],
            node_selectors=self.kubernetes_execution_parameters.node_selectors,
            tolerations=self.kubernetes_execution_parameters.tolerations,
            annotations=self.kubernetes_execution_parameters.annotations,
            arguments=args,
            labels=self.kubernetes_execution_parameters.labels,
            name=name,
            task_id=name,
            resources=self.kubernetes_execution_parameters.get_resources(),
            env_vars=self.kubernetes_execution_parameters.env_vars,
            secrets=self.kubernetes_execution_parameters.secrets,
            is_delete_operator_pod=self.kubernetes_execution_parameters.is_delete_operator_pod,
            hostnetwork=False,
        )
