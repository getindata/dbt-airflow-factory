"""Class parsing ``manifest.json`` into Airflow tasks."""

import json
import logging
from typing import Any, ContextManager, Dict, KeysView, List, Tuple

import airflow
from airflow.models.baseoperator import BaseOperator

if not airflow.__version__.startswith("1."):
    from airflow.utils.task_group import TaskGroup

from dbt_airflow_factory.operator import DbtRunOperatorBuilder, EphemeralOperator
from dbt_airflow_factory.tasks import ModelExecutionTask, ModelExecutionTasks


class DbtAirflowTasksBuilder:
    """
    Parses ``manifest.json`` into Airflow tasks.

    :param operator_builder: DBT node operator.
    :type operator_builder: DbtRunOperatorBuilder
    """

    def __init__(self, operator_builder: DbtRunOperatorBuilder):
        self.operator_builder = operator_builder

    class ModelTestsTuple:
        def __init__(
            self,
            run_test_tasks: Dict[str, ModelExecutionTask],
            multiple_dependency_test_tasks: Dict[Tuple[str, ...], ModelExecutionTask],
        ) -> None:
            self._run_test_tasks = run_test_tasks
            self._multiple_dependency_test_tasks = multiple_dependency_test_tasks

        @property
        def result_tasks(self) -> Dict[str, ModelExecutionTask]:
            result_tasks = dict(self._run_test_tasks)
            for task_test_tuple, task_test in self._multiple_dependency_test_tasks.items():
                result_tasks[
                    DbtAirflowTasksBuilder._build_multiple_deps_test_name(task_test_tuple)
                ] = task_test
            return result_tasks

        @property
        def run_test_tasks_keys(self) -> KeysView[str]:
            return self._run_test_tasks.keys()

        @property
        def multiple_dependency_test_tasks_keys(self) -> KeysView[Tuple[str, ...]]:
            return self._multiple_dependency_test_tasks.keys()

    @staticmethod
    def _load_dbt_manifest(manifest_path: str) -> dict:
        with open(manifest_path, "r") as f:
            manifest_content = json.load(f)
            logging.debug("Manifest content: " + str(manifest_content))
            return manifest_content

    def _make_dbt_test_task(self, model_name: str, is_in_task_group: bool) -> BaseOperator:
        command = "test"
        return self.operator_builder.create(
            self._build_task_name(model_name, command, is_in_task_group),
            command,
            model_name,
            additional_dbt_args=["--indirect-selection=cautious"],
        )

    def _make_dbt_multiple_deps_test_task(
        self, test_names: List[str], dependencies: tuple
    ) -> BaseOperator:
        command = "test"
        return self.operator_builder.create(
            self._build_multiple_deps_test_name(dependencies),
            command,
            " ".join(test_names),
        )

    def _make_dbt_run_task(self, model_name: str, is_in_task_group: bool) -> BaseOperator:
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
    def _build_multiple_deps_test_name(dependencies: tuple) -> str:
        return "_".join(map(lambda node_name: node_name.split(".")[-1], dependencies)) + "_test"

    @staticmethod
    def _is_task_type(node_name: str, task_type: str) -> bool:
        return node_name.split(".")[0] == task_type

    @staticmethod
    def _is_model_run_task(node_name: str) -> bool:
        return DbtAirflowTasksBuilder._is_task_type(node_name, "model")

    @staticmethod
    def _is_test_task(node_name: str) -> bool:
        return DbtAirflowTasksBuilder._is_task_type(node_name, "test")

    @staticmethod
    def _is_ephemeral_task(node: dict) -> bool:
        return node["config"]["materialized"] == "ephemeral"

    @staticmethod
    def _create_task_group_for_model(
        model_name: str, use_task_group: bool
    ) -> Tuple[Any, ContextManager]:
        import contextlib

        is_first_version = airflow.__version__.startswith("1.")
        task_group = (
            None if (is_first_version or not use_task_group) else TaskGroup(group_id=model_name)
        )
        task_group_ctx = task_group or contextlib.nullcontext()
        return task_group, task_group_ctx

    def _create_task_for_model(
        self,
        model_name: str,
        is_ephemeral_task: bool,
        use_task_group: bool,
    ) -> ModelExecutionTask:
        if is_ephemeral_task:
            return ModelExecutionTask(EphemeralOperator(task_id=f"{model_name}__ephemeral"), None)

        (task_group, task_group_ctx) = self._create_task_group_for_model(model_name, use_task_group)
        is_in_task_group = task_group is not None
        with task_group_ctx:
            run_task = self._make_dbt_run_task(model_name, is_in_task_group)
            test_task = self._make_dbt_test_task(model_name, is_in_task_group)
            # noinspection PyStatementEffect
            run_task >> test_task
        return ModelExecutionTask(run_task, test_task, task_group)

    def _create_tasks_for_each_multiple_deps_test(
        self, manifest: dict
    ) -> Dict[Tuple[str, ...], ModelExecutionTask]:
        tests_with_more_deps: Dict[tuple, List[str]] = {}
        for node in [v for k, v in manifest["nodes"].items() if self._is_test_task(k)]:
            model_dependencies = list(filter(self._is_model_run_task, node["depends_on"]["nodes"]))
            if len(model_dependencies) <= 1:
                continue

            model_dependencies.sort()
            if tuple(model_dependencies) not in tests_with_more_deps:
                tests_with_more_deps[tuple(model_dependencies)] = []
            tests_with_more_deps[tuple(model_dependencies)].append(node["name"])

        return {
            depends_on_tuple: ModelExecutionTask(
                self._make_dbt_multiple_deps_test_task(test_names, depends_on_tuple), None
            )
            for depends_on_tuple, test_names in tests_with_more_deps.items()
        }

    def _create_tasks_for_each_model(
        self, manifest: dict, use_task_group: bool
    ) -> Dict[str, ModelExecutionTask]:
        tasks = {}

        for node_name, node in [
            (str(k), v) for k, v in manifest["nodes"].items() if self._is_model_run_task(k)
        ]:
            logging.info("Creating tasks for: " + node_name)
            tasks[node_name] = self._create_task_for_model(
                node_name.split(".")[-1], self._is_ephemeral_task(node), use_task_group
            )

        return tasks

    @staticmethod
    def _create_task_dependency(
        upstream_node: str,
        downstream_node: str,
        result_tasks: Dict[str, ModelExecutionTask],
        starting_tasks: List[str],
        ending_tasks: List[str],
    ) -> None:
        # noinspection PyStatementEffect
        (
            result_tasks[upstream_node].get_end_task()
            >> result_tasks[downstream_node].get_start_task()
        )
        if downstream_node in starting_tasks:
            starting_tasks.remove(downstream_node)
        if upstream_node in ending_tasks:
            ending_tasks.remove(upstream_node)

    def _create_tasks_dependencies(
        self,
        manifest: dict,
        model_tests_tuple: ModelTestsTuple,
    ) -> ModelExecutionTasks:
        result_tasks = model_tests_tuple.result_tasks
        starting_tasks = list(result_tasks.keys())
        ending_tasks = list(result_tasks.keys())

        for node_name in model_tests_tuple.run_test_tasks_keys:
            for upstream_node in manifest["nodes"][node_name]["depends_on"]["nodes"]:
                if self._is_model_run_task(upstream_node):
                    self._create_task_dependency(
                        upstream_node, node_name, result_tasks, starting_tasks, ending_tasks
                    )

        for depends_on_tuple in model_tests_tuple.multiple_dependency_test_tasks_keys:
            node_name = self._build_multiple_deps_test_name(depends_on_tuple)
            for upstream_node in depends_on_tuple:
                self._create_task_dependency(
                    upstream_node, node_name, result_tasks, starting_tasks, ending_tasks
                )

        return ModelExecutionTasks(result_tasks, starting_tasks, ending_tasks)

    def _make_dbt_tasks(self, manifest_path: str, use_task_group: bool) -> ModelExecutionTasks:
        manifest = self._load_dbt_manifest(manifest_path)
        model_tests_tuple = self.ModelTestsTuple(
            self._create_tasks_for_each_model(manifest, use_task_group),
            self._create_tasks_for_each_multiple_deps_test(manifest),
        )
        tasks_with_context = self._create_tasks_dependencies(manifest, model_tests_tuple)
        logging.info(f"Created {str(tasks_with_context.length())} tasks groups")
        return tasks_with_context

    def parse_manifest_into_tasks(
        self, manifest_path: str, use_task_group: bool = True
    ) -> ModelExecutionTasks:
        """
        Parse ``manifest.json`` into tasks.

        :param manifest_path: Path to ``manifest.json``.
        :type manifest_path: str
        :param use_task_group: Whether to use TaskGroup (does not work in Airflow 1).
        :type use_task_group: bool
        :return: Dictionary of tasks created from ``manifest.json`` parsing.
        :rtype: ModelExecutionTasks
        """
        return self._make_dbt_tasks(manifest_path, use_task_group)

    def create_seed_task(self) -> BaseOperator:
        """
        Create ``dbt_seed`` task.

        :return: Operator for ``dbt_seed`` task.
        :rtype: BaseOperator
        """
        return self.operator_builder.create("dbt_seed", "seed")
