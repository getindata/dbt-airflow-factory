import json
import os
import tempfile
from datetime import datetime

import airflow
from airflow import DAG

from dbt_airflow_factory.builder_factory import DbtAirflowTasksBuilderFactory


def manifest_file_with_models(nodes_with_dependencies):
    content_nodes = {}
    for node_name in nodes_with_dependencies.keys():
        content_nodes[node_name] = {
            "depends_on": {"nodes": nodes_with_dependencies[node_name]},
            "config": {"materialized": "view"},
        }
    content = {"nodes": content_nodes}
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(str.encode(json.dumps(content)))
        return tmp.name


def builder_factory():
    return DbtAirflowTasksBuilderFactory(os.path.dirname(os.path.abspath(__file__)), "dev")


def test_dag():
    return DAG("test", default_args={"start_date": datetime(2021, 10, 13)})


IS_FIRST_AIRFLOW_VERSION = airflow.__version__.startswith("1.")


def task_group_prefix_builder(task_model_id: str, task_command: str):
    return (
        f"{task_model_id}_{task_command}"
        if IS_FIRST_AIRFLOW_VERSION
        else f"{task_model_id}.{task_command}"
    )
