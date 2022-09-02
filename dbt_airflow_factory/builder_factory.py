"""Factory creating Airflow tasks."""

from dbt_airflow_factory.config_utils import read_config
from dbt_airflow_factory.dbt_parameters import DbtExecutionEnvironmentParameters
from dbt_airflow_factory.ecs.ecs_operator import EcsPodOperatorBuilder
from dbt_airflow_factory.ecs.ecs_parameters_loader import EcsExecutionParametersLoader
from dbt_airflow_factory.k8s.k8s_operator import KubernetesPodOperatorBuilder
from dbt_airflow_factory.k8s.k8s_parameters_loader import (
    KubernetesExecutionParametersLoader,
)
from dbt_airflow_factory.operator import DbtRunOperatorBuilder
from dbt_airflow_factory.tasks_builder.builder import DbtAirflowTasksBuilder
from dbt_airflow_factory.tasks_builder.gateway import (
    GatewayConfiguration,
    TaskGraphConfiguration,
)
from dbt_airflow_factory.tasks_builder.parameters import TasksBuildingParameters


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
    :param execution_env_config_file_name: name of the execution environment config file.
        If not specified, default value is ``execution_env.yml``.
    :type execution_env_config_file_name: str
    """

    base_config_name: str
    """Name of the ``base`` environment (default: ``base``)."""
    dag_path: str
    """path to ``manifest.json`` file."""
    env: str
    """name of the environment."""
    dbt_config_file_name: str
    """name of the DBT config file (default: ``dbt.yml``)."""
    execution_env_config_file_name: str
    """name of the execution env config file (default: ``execution_env.yml``)."""

    def __init__(
        self,
        dag_path: str,
        env: str,
        airflow_config: dict,
        dbt_config_file_name: str = "dbt.yml",
        execution_env_config_file_name: str = "execution_env.yml",
    ):
        self.base_config_name = "base"
        self.dag_path = dag_path
        self.env = env
        self.airflow_config = airflow_config
        self.dbt_config_file_name = dbt_config_file_name
        self.execution_env_config_file_name = execution_env_config_file_name

    def create(self) -> DbtAirflowTasksBuilder:
        """
        Create :class:`.DbtAirflowTasksBuilder` to use.

        :return: Instance of :class:`.DbtAirflowTasksBuilder`.
        :rtype: DbtAirflowTasksBuilder
        """
        dbt_params = self._create_dbt_config()
        execution_env_type = self._read_execution_env_type()
        tasks_airflow_config = self._create_tasks_airflow_config()

        return DbtAirflowTasksBuilder(
            tasks_airflow_config,
            self._create_operator_builder(execution_env_type, dbt_params),
            gateway_config=TaskGraphConfiguration(
                GatewayConfiguration(
                    separation_schemas=self.airflow_config.get("save_points", []),
                    gateway_task_name="gateway",
                )
            ),
        )

    def _create_tasks_airflow_config(self) -> TasksBuildingParameters:
        return TasksBuildingParameters(
            self.airflow_config.get("use_task_group", False),
            self.airflow_config.get("show_ephemeral_models", True),
            self.airflow_config.get("enable_project_dependencies", False),
        )

    def _create_operator_builder(
        self, type: str, dbt_params: DbtExecutionEnvironmentParameters
    ) -> DbtRunOperatorBuilder:
        if type == "k8s":
            return KubernetesPodOperatorBuilder(
                dbt_params,
                KubernetesExecutionParametersLoader.create_config(
                    self.dag_path, self.env, self.execution_env_config_file_name
                ),
            )
        elif type == "ecs":
            return EcsPodOperatorBuilder(
                dbt_params,
                EcsExecutionParametersLoader.create_config(
                    self.dag_path, self.env, self.execution_env_config_file_name
                ),
            )
        else:
            raise TypeError(f"Unsupported env type {type}")

    def _create_dbt_config(self) -> DbtExecutionEnvironmentParameters:
        return DbtExecutionEnvironmentParameters(
            **read_config(self.dag_path, self.env, self.dbt_config_file_name)
        )

    def _read_execution_env_type(self) -> str:
        return read_config(self.dag_path, self.env, self.execution_env_config_file_name)["type"]
