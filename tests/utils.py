import json
import tempfile
from datetime import datetime

from airflow import DAG
from airflow.kubernetes.secret import Secret

from dbt_airflow_manifest_parser.builder import DbtAirflowTasksBuilder
from dbt_airflow_manifest_parser.operator import KubernetesPodOperatorBuilder
from dbt_airflow_manifest_parser.parameters import (
    DbtExecutionEnvironmentParameters,
    KubernetesExecutionParameters,
)


def manifest_file_with_models(nodes_with_dependencies):
    content_nodes = {}
    for node_name in nodes_with_dependencies.keys():
        content_nodes[node_name] = {
            "depends_on": {"nodes": nodes_with_dependencies[node_name]}
        }
    content = {"nodes": content_nodes}
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(str.encode(json.dumps(content)))
        return tmp.name


def execution_environment_parameters():
    return DbtExecutionEnvironmentParameters(
        target="dev", project_dir_path="/dbt", profile_dir_path="/root/.dbt"
    )


def kubernetes_parameters():
    return KubernetesExecutionParameters(
        namespace="apache-airflow",
        image="dbt-platform-poc:123",
        node_selectors={"group": "data-processing"},
        annotations={"iam.amazonaws.com/role": "k8s-airflow"},
        tolerations=[
            {
                "key": "group",
                "operator": "Equal",
                "value": "data-processing",
                "effect": "NoSchedule",
            },
        ],
        labels={"runner": "airflow"},
        limit_resources={"memory": "2048M", "cpu": "2"},
        requested_resources={"memory": "1024M", "cpu": "1"},
        secrets=[
            Secret("env", None, "snowflake-access-user-key", None),
            Secret("volume", "/var", "snowflake-access-user-key", None),
        ],
        is_delete_operator_pod=True,
    )


def task_builder():
    return DbtAirflowTasksBuilder(
        KubernetesPodOperatorBuilder(
            execution_environment_parameters(), kubernetes_parameters()
        )
    )


def test_dag():
    return DAG("test", default_args={"start_date": datetime(2021, 10, 13)})
