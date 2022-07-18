"""Factories creating Airflow Operators running DBT tasks."""

from typing import List, Optional

import airflow

from dbt_airflow_factory.dbt_parameters import DbtExecutionEnvironmentParameters
from dbt_airflow_factory.k8s.k8s_parameters import KubernetesExecutionParameters
from dbt_airflow_factory.operator import DbtRunOperatorBuilder

if airflow.__version__.startswith("1."):
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
else:
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
        KubernetesPodOperator,
    )

from airflow.models.baseoperator import BaseOperator


class KubernetesPodOperatorBuilder(DbtRunOperatorBuilder):
    """
    Builder of Kubernetes Pod Operator running a single DBT task.

    :param dbt_execution_env_parameters: POD representing DBT operator config file.
    :type dbt_execution_env_parameters: DbtExecutionEnvironmentParameters
    :param kubernetes_execution_parameters:
        POD representing Kubernetes operator config file.
    :type kubernetes_execution_parameters: KubernetesExecutionParameters
    """

    dbt_execution_env_parameters: DbtExecutionEnvironmentParameters
    """POD representing DBT operator config file."""
    kubernetes_execution_parameters: KubernetesExecutionParameters
    """POD representing Kubernetes operator config file."""

    def __init__(
        self,
        dbt_execution_env_parameters: DbtExecutionEnvironmentParameters,
        kubernetes_execution_parameters: KubernetesExecutionParameters,
    ):
        self.dbt_execution_env_parameters = dbt_execution_env_parameters
        self.kubernetes_execution_parameters = kubernetes_execution_parameters

    def create(
        self,
        name: str,
        command: str,
        model: Optional[str] = None,
        additional_dbt_args: Optional[List[str]] = None,
    ) -> BaseOperator:
        return self._create(self._prepare_arguments(command, model, additional_dbt_args), name)

    def _prepare_arguments(
        self,
        command: str,
        model: Optional[str],
        additional_dbt_args: Optional[List[str]],
    ) -> List[str]:
        args = [
            f"{self.kubernetes_execution_parameters.execution_script}",
            f"{command}",
            f"--target {self.dbt_execution_env_parameters.target}",
            f'--vars "{self.dbt_execution_env_parameters.vars}"',
            f"--project-dir {self.dbt_execution_env_parameters.project_dir_path}",
            f"--profiles-dir {self.dbt_execution_env_parameters.profile_dir_path}",
        ]
        if model:
            args += [f"--select {model}"]
        if additional_dbt_args:
            args += additional_dbt_args
        return [" ".join(args)]

    def _create(self, args: Optional[List[str]], name: str) -> KubernetesPodOperator:
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
            resources=self.kubernetes_execution_parameters.resources,
            env_vars=self.kubernetes_execution_parameters.env_vars,
            secrets=self.kubernetes_execution_parameters.secrets,
            is_delete_operator_pod=self.kubernetes_execution_parameters.is_delete_operator_pod,  # noqa: E501
            hostnetwork=False,
            config_file=self.kubernetes_execution_parameters.config_file,
        )
