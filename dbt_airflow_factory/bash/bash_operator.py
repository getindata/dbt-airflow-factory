"""Factories creating Airflow Operators running DBT tasks."""

from typing import List, Optional

from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash_operator import BashOperator

from dbt_airflow_factory.bash.bash_parameters import BashExecutionParameters
from dbt_airflow_factory.dbt_parameters import DbtExecutionEnvironmentParameters
from dbt_airflow_factory.operator import DbtRunOperatorBuilder


class BashOperatorBuilder(DbtRunOperatorBuilder):
    """
    Builder of Bash Operator running a single DBT task.

    :param dbt_execution_env_parameters: POD representing DBT operator config file.
    :type dbt_execution_env_parameters: DbtExecutionEnvironmentParameters
    :param bash_execution_parameters:
        POD representing bash execution parameters.
    :type bash_execution_parameters: BashExecutionParameters
    """

    dbt_execution_env_parameters: DbtExecutionEnvironmentParameters
    """POD representing DBT operator config file."""
    bash_execution_parameters: BashExecutionParameters
    """POD representing bash execution parameters."""

    def __init__(
        self,
        dbt_execution_env_parameters: DbtExecutionEnvironmentParameters,
        bash_execution_parameters: BashExecutionParameters,
    ):
        self.dbt_execution_env_parameters = dbt_execution_env_parameters
        self.bash_execution_parameters = bash_execution_parameters

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
            f"{self.bash_execution_parameters.execution_script}",
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

    def _create(self, args: List[str], name: str) -> BashOperator:
        return BashOperator(bash_command=" ".join(args), task_id=name)
