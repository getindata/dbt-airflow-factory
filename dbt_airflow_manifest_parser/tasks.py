from typing import List

from airflow.models.baseoperator import BaseOperator


class ModelExecutionTask:
    def __init__(self, run_airflow_task: BaseOperator, test_airflow_task: BaseOperator):
        self.run_airflow_task = run_airflow_task
        self.test_airflow_task = test_airflow_task

    # todo only for airflow 2.x
    def to_task_group(self):
        return None


class ModelExecutionTasks:
    def __init__(
        self,
        tasks: List[ModelExecutionTask],
        starting_task_names: List[str],
        ending_task_names: List[str],
    ):
        self._tasks = tasks
        self._starting_task_names = starting_task_names
        self._ending_task_names = ending_task_names

    def get_task(self, node_name) -> ModelExecutionTask:
        return self._tasks[node_name]

    def length(self) -> int:
        return len(self._tasks)

    def get_starting_tasks(self) -> List[ModelExecutionTask]:
        return self._extract_by_keys(self._starting_task_names)

    def get_ending_tasks(self) -> List[ModelExecutionTask]:
        return self._extract_by_keys(self._ending_task_names)

    def _extract_by_keys(self, keys) -> List[ModelExecutionTask]:
        tasks = []
        for key in keys:
            tasks.append(self._tasks[key])
        return tasks
