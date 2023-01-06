from dbt_airflow_factory.bash.bash_parameters import BashExecutionParameters
from dbt_airflow_factory.config_utils import read_config


class BashExecutionParametersLoader:
    @staticmethod
    def create_config(
        dag_path: str, env: str, execution_env_config_file_name: str
    ) -> BashExecutionParameters:
        config = read_config(dag_path, env, "bash.yml")
        config.update(read_config(dag_path, env, execution_env_config_file_name))
        return BashExecutionParameters(**config)
