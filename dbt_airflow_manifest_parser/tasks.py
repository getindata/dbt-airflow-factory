from typing import Dict, List

from airflow.models.baseoperator import BaseOperator


class ModelExecutionTask:
    def __init__(
        self,
        run_airflow_task: BaseOperator,
        test_airflow_task: BaseOperator,
        task_group=None,
    ):
        self.run_airflow_task = run_airflow_task
        self.test_airflow_task = test_airflow_task
        self.task_group = task_group

    def __repr__(self):
        return repr(self.task_group) or repr(
            [self.run_airflow_task, self.test_airflow_task]
        )

    def get_start_task(self):
        return self.task_group or self.run_airflow_task

    def get_end_task(self):
        return self.task_group or self.test_airflow_task


class ModelExecutionTasks:
    def __init__(
        self,
        tasks: Dict[str, ModelExecutionTask],
        starting_task_names: List[str],
        ending_task_names: List[str],
    ):
        self._tasks = tasks
        self._starting_task_names = starting_task_names
        self._ending_task_names = ending_task_names

    def __repr__(self):
        return f"ModelExecutionTasks(\n {self._tasks} \n)"

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
