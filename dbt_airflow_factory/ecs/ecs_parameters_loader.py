"""POD representing Kubernetes operator config file."""

from dbt_airflow_factory.ecs.ecs_parameters import EcsExecutionParameters


class EcsExecutionParametersLoader:
    @staticmethod
    def create_config(
        dag_path: str, env: str, execution_env_config_file_name: str
    ) -> EcsExecutionParameters:
        raise NotImplementedError
