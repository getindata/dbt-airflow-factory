class ModelExecutionTasks:
    def __init__(self, tasks, starting_task_names, ending_task_names):
        self._tasks = tasks
        self._starting_task_names = starting_task_names
        self._ending_task_names = ending_task_names

    def get_task(self, node_name):
        return self._tasks[node_name]

    def length(self):
        return len(self._tasks)

    def get_starting_tasks(self):
        return self._extract_by_keys(self._starting_task_names)

    def get_ending_tasks(self):
        return self._extract_by_keys(self._ending_task_names)

    def _extract_by_keys(self, keys):
        tasks = []
        for key in keys:
            tasks.append(self._tasks[key])
        return tasks


class ModelExecutionTask:
    def __init__(self, run_airflow_task, test_airflow_task):
        self.run_airflow_task = run_airflow_task
        self.test_airflow_task = test_airflow_task

    # todo only for airflow 2.x
    def to_task_group(self):
        return None
