import json
import os

import matplotlib
import networkx
from matplotlib import pyplot as plt

from dbt_airflow_factory.builder_factory import DbtAirflowTasksBuilderFactory
from dbt_airflow_factory.tasks_builder.graph import DbtAirflowGraph, TaskGraphConfiguration, GatewayConfiguration
from .utils import (
    builder_factory,
    manifest_file_with_models,
    task_group_prefix_builder,
    test_dag,
)


def test_starting_tasks():
    # given
    manifest_path = manifest_file_with_models(
        {
            "model.dbt_test.model1": [],
            "model.dbt_test.model2": [],
            "model.dbt_test.model3": ["model.dbt_test.model1", "model.dbt_test.model2"],
            "model.dbt_test.model4": ["model.dbt_test.model3"],
            "model.dbt_test.model5": [],
        }
    )

    # when
    with test_dag():
        tasks = builder_factory().create().parse_manifest_into_tasks(manifest_path)

    # then
    starting_tasks_names = [
        task.execution_airflow_task.task_id for task in tasks.get_starting_tasks()
    ]
    assert task_group_prefix_builder("model1", "run") in starting_tasks_names
    assert task_group_prefix_builder("model2", "run") in starting_tasks_names
    assert task_group_prefix_builder("model5", "run") in starting_tasks_names


def test_ending_tasks():
    # given
    manifest_path = manifest_file_with_models(
        {
            "model.dbt_test.model1": [],
            "model.dbt_test.model2": [],
            "model.dbt_test.model3": ["model.dbt_test.model1", "model.dbt_test.model2"],
            "model.dbt_test.model4": ["model.dbt_test.model3"],
            "model.dbt_test.model5": [],
        }
    )

    # when
    with test_dag():
        tasks = DbtAirflowTasksBuilderFactory(
            os.path.dirname(os.path.abspath(__file__)),
            "dev",
            {
                "save_points": []
            },
        ).create().parse_manifest_into_tasks(manifest_path)

    # then
    ending_tasks_names = [task.test_airflow_task.task_id for task in tasks.get_ending_tasks()]
    assert task_group_prefix_builder("model4", "test") in ending_tasks_names
    assert task_group_prefix_builder("model5", "test") in ending_tasks_names


def test_manifest_with_gateway():
    # given manifest path

    # ag = DbtAirflowGraph(
    #     TaskGraphConfiguration(
    #         gateway=GatewayConfiguration(
    #             production_schema_name="PAWEL_PRIVATE_SCHEMA",
    #             staging_schema_name="PAWEL_PRIVATE_SCHEMA_STG"
    #         )
    #     )
    # )
    # plt.interactive(False)
    # plt.show(block=True)
    # matplotlib.use('TkAgg')
    # ag.add_execution_tasks(manifest_data)
    # ag.add_external_dependencies(manifest)
    # ag.create_edges_from_dependencies(manifest_data)
    # networkx.draw(ag.graph)
    #
    # dbt_airflow_graph.create_edges_from_dependencies(
    #     self.airflow_config.enable_dags_dependencies
    # )
    # if not self.airflow_config.show_ephemeral_models:
    #     dbt_airflow_graph.remove_ephemeral_nodes_from_graph()
    # dbt_airflow_graph.contract_test_nodes()

    with test_dag():
        tasks = DbtAirflowTasksBuilderFactory(
            os.path.dirname(os.path.abspath(__file__)),
            "dev",
            {
                "save_points": ["PAWEL_PRIVATE_SCHEMA_STG", "PAWEL_PRIVATE_SCHEMA"]
            },
        ).create().parse_manifest_into_tasks("/Users/pawel.kocinski/Desktop/projects/datalab/forks/dbt-airflow-factory/tests/manifest_volt2.json")

    print("s")

def test_valid_gateway_dependency():
    pass