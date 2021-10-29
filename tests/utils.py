import os
import json
import tempfile
from datetime import datetime

from airflow import DAG
from dbt_airflow_manifest_parser.builder_factory import DbtAirflowTasksBuilderFactory


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


def builder_factory():
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')
    return DbtAirflowTasksBuilderFactory(config_path, 'dev')


def test_dag():
    return DAG("test", default_args={"start_date": datetime(2021, 10, 13)})
