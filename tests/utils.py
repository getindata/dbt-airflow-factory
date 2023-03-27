import json
import os
import tempfile
from datetime import datetime

from airflow import DAG

from dbt_airflow_factory.builder_factory import DbtAirflowTasksBuilderFactory


def manifest_file_with_models(nodes_with_dependencies: dict, extra_metadata: dict = None):
    content_nodes = {}
    for node_name in nodes_with_dependencies.keys():
        content_nodes[node_name] = {
            "depends_on": {"nodes": nodes_with_dependencies[node_name]},
            "config": {"materialized": "view"},
            "name": node_name.split(".")[-1],
        }
    content = {"nodes": content_nodes, "child_map": {}}
    if extra_metadata:
        content.update(extra_metadata)
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(str.encode(json.dumps(content)))
        return tmp.name


def builder_factory(use_task_group=True, enable_project_dependencies=False, env="dev"):
    return DbtAirflowTasksBuilderFactory(
        os.path.dirname(os.path.abspath(__file__)),
        env,
        {
            "enable_project_dependencies": enable_project_dependencies,
            "use_task_group": use_task_group,
        },
    )


def test_dag():
    return DAG("test", default_args={"start_date": datetime(2021, 10, 13)})


def task_group_prefix_builder(task_model_id: str, task_command: str) -> str:
    from dbt_airflow_factory.constants import IS_FIRST_AIRFLOW_VERSION

    return (
        f"{task_model_id}_{task_command}"
        if IS_FIRST_AIRFLOW_VERSION
        else f"{task_model_id}.{task_command}"
    )
