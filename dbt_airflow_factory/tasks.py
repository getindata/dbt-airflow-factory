"""Classes representing tasks corresponding to a single DBT model."""

from typing import Dict, Iterable, List, Optional

from airflow.models.baseoperator import BaseOperator


class ModelExecutionTask:
    """
    Wrapper around tasks corresponding to a single DBT model.

    :param run_airflow_task: Operator running DBT's ``run`` task.
    :type run_airflow_task: BaseOperator
    :param test_airflow_task: Operator running DBT's ``test`` task (optional).
    :type test_airflow_task: BaseOperator
    :param task_group: TaskGroup consisting of ``run`` and ``test`` tasks
        (if Airflow version is at least 2).
    """

    def __init__(  # type: ignore
        self,
        run_airflow_task: BaseOperator,
        test_airflow_task: Optional[BaseOperator],
        task_group=None,
    ) -> None:
        self.run_airflow_task = run_airflow_task
        self.test_airflow_task = test_airflow_task
        self.task_group = task_group

    def __repr__(self) -> str:
        return (
            repr(self.task_group)
            if self.task_group
            else repr(
                [self.run_airflow_task]
                + ([self.test_airflow_task] if self.test_airflow_task else [])
            )
        )

    def get_start_task(self):  # type: ignore
        """
        Return model's first task.

        It is either a whole TaskGroup or ``run`` task.
        """
        return self.task_group or self.run_airflow_task

    def get_end_task(self):  # type: ignore
        """
        Return model's last task.

        It is either a whole TaskGroup, ``test`` task, or ``run`` task, depending
        on version of Airflow and existence of ``test`` task.
        """
        return self.task_group or self.test_airflow_task or self.run_airflow_task


class ModelExecutionTasks:
    """
    Dictionary of all Operators corresponding to DBT tasks.

    :param tasks: Dictionary of model tasks.
    :type tasks: Dict[str, ModelExecutionTask]
    :param starting_task_names: List of names of initial tasks (DAG sources).
    :type starting_task_names: List[str]
    :param ending_task_names: List of names of ending tasks (DAG sinks).
    :type ending_task_names: List[str]
    """

    def __init__(
        self,
        tasks: Dict[str, ModelExecutionTask],
        starting_task_names: List[str],
        ending_task_names: List[str],
    ) -> None:
        self._tasks = tasks
        self._starting_task_names = starting_task_names
        self._ending_task_names = ending_task_names

    def __repr__(self) -> str:
        return f"ModelExecutionTasks(\n {self._tasks} \n)"

    def get_task(self, node_name: str) -> ModelExecutionTask:
        """
        Return :class:`ModelExecutionTask` for given model's **node_name**.

        :param node_name: Name of the task.
        :type node_name: str
        :return: Wrapper around tasks corresponding to a given model.
        :rtype: ModelExecutionTask
        """
        return self._tasks[node_name]

    def length(self) -> int:
        """Count TaskGroups corresponding to a single DBT model."""
        return len(self._tasks)

    def get_starting_tasks(self) -> List[ModelExecutionTask]:
        """
        Get a list of all DAG sources.

        :return: List of all DAG sources.
        :rtype: List[ModelExecutionTask]
        """
        return self._extract_by_keys(self._starting_task_names)

    def get_ending_tasks(self) -> List[ModelExecutionTask]:
        """
        Get a list of all DAG sinks.

        :return: List of all DAG sinks.
        :rtype: List[ModelExecutionTask]
        """
        return self._extract_by_keys(self._ending_task_names)

    def _extract_by_keys(self, keys: Iterable[str]) -> List[ModelExecutionTask]:
        tasks = []
        for key in keys:
            tasks.append(self._tasks[key])
        return tasks
