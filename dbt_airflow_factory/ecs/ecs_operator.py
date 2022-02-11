"""Factories creating Airflow Operators running DBT tasks."""

from typing import List, Optional

from airflow.models.baseoperator import BaseOperator

from dbt_airflow_factory.dbt_parameters import DbtExecutionEnvironmentParameters
from dbt_airflow_factory.ecs.ecs_parameters import EcsExecutionParameters
from dbt_airflow_factory.operator import DbtRunOperatorBuilder


class EcsPodOperatorBuilder(DbtRunOperatorBuilder):

    dbt_execution_env_parameters: DbtExecutionEnvironmentParameters
    """POD representing DBT operator config file."""
    ecs_execution_parameters: EcsExecutionParameters
    """POD representing ecs operator config file."""

    def __init__(
        self,
        dbt_execution_env_parameters: DbtExecutionEnvironmentParameters,
        ecs_execution_parameters: EcsExecutionParameters,
    ):
        self.dbt_execution_env_parameters = dbt_execution_env_parameters
        self.ecs_execution_parameters = ecs_execution_parameters

    def create(
        self,
        name: str,
        command: str,
        model: Optional[str] = None,
        additional_dbt_args: Optional[List[str]] = None,
    ) -> BaseOperator:
        raise NotImplementedError  # TODO
