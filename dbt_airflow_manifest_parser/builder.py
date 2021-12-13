import json
import logging
import typing

import airflow
from airflow.models.baseoperator import BaseOperator

if not airflow.__version__.startswith("1."):
    from airflow.utils.task_group import TaskGroup

from dbt_airflow_manifest_parser.operator import DbtRunOperatorBuilder
from dbt_airflow_manifest_parser.tasks import ModelExecutionTask, ModelExecutionTasks


class DbtAirflowTasksBuilder:
    def __init__(self, operator_builder: DbtRunOperatorBuilder):
        self.operator_builder = operator_builder

    def _load_dbt_manifest(self, manifest_path: str) -> dict:
        with open(manifest_path, "r") as f:
            manifest_content = json.load(f)
            logging.debug("Manifest content: " + str(manifest_content))
            return manifest_content

    def _make_dbt_test_task(
        self, model_name: str, is_in_task_group: bool
    ) -> BaseOperator:
        command = "test"
        return self.operator_builder.create(
            self._build_task_name(model_name, command, is_in_task_group),
            command,
            model_name,
        )

    def _make_dbt_run_task(
        self, model_name: str, is_in_task_group: bool
    ) -> BaseOperator:
        command = "run"
        return self.operator_builder.create(
            self._build_task_name(model_name, command, is_in_task_group),
            command,
            model_name,
        )

    @staticmethod
    def _build_task_name(model_name: str, command: str, is_in_task_group: bool) -> str:
        return command if is_in_task_group else f"{model_name}_{command}"

    @staticmethod
    def _is_model_run_task(node_name: str) -> bool:
        return node_name.split(".")[0] == "model"

    @staticmethod
    def _create_task_group_for_model(model_name: str, use_task_group: bool):
        import contextlib

        is_first_version = airflow.__version__.startswith("1.")
        task_group = (
            None
            if (is_first_version or not use_task_group)
            else TaskGroup(group_id=model_name)
        )
        task_group_ctx = task_group or contextlib.nullcontext()
        return task_group, task_group_ctx

    def _create_tasks_for_each_model(
        self, manifest: dict, use_task_group: bool
    ) -> dict:
        tasks = {}
        for node_name in manifest["nodes"].keys():
            if self._is_model_run_task(node_name):
                logging.info("Creating tasks for: " + node_name)
                model_name = node_name.split(".")[-1]
                (task_group, task_group_ctx) = self._create_task_group_for_model(
                    model_name, use_task_group
                )
                is_in_task_group = task_group is not None
                with task_group_ctx:
                    run_task = self._make_dbt_run_task(model_name, is_in_task_group)
                    test_task = self._make_dbt_test_task(model_name, is_in_task_group)
                    # noinspection PyStatementEffect
                    run_task >> test_task
                tasks[node_name] = ModelExecutionTask(run_task, test_task, task_group)
        return tasks

    def _create_tasks_dependencies(
        self, manifest: dict, tasks: typing.Dict[str, ModelExecutionTask]
    ) -> ModelExecutionTasks:
        starting_tasks = list(tasks.keys())
        ending_tasks = list(tasks.keys())
        for node_name in tasks.keys():
            for upstream_node in manifest["nodes"][node_name]["depends_on"]["nodes"]:
                if self._is_model_run_task(upstream_node):
                    # noinspection PyStatementEffect
                    (
                        tasks[upstream_node].get_end_task()
                        >> tasks[node_name].get_start_task()
                    )
                    if node_name in starting_tasks:
                        starting_tasks.remove(node_name)
                    if upstream_node in ending_tasks:
                        ending_tasks.remove(upstream_node)
        return ModelExecutionTasks(tasks, starting_tasks, ending_tasks)

    def _make_dbt_tasks(
        self, manifest_path: str, use_task_group: bool
    ) -> ModelExecutionTasks:
        manifest = self._load_dbt_manifest(manifest_path)
        tasks = self._create_tasks_for_each_model(manifest, use_task_group)
        tasks_with_context = self._create_tasks_dependencies(manifest, tasks)
        logging.info(f"Created {str(tasks_with_context.length())} tasks groups")
        return tasks_with_context

    def parse_manifest_into_tasks(
        self, manifest_path: str, use_task_group: bool = True
    ) -> ModelExecutionTasks:
        return self._make_dbt_tasks(manifest_path, use_task_group)

    def create_seed_task(self) -> BaseOperator:
        return self.operator_builder.create("dbt_seed", "seed")
