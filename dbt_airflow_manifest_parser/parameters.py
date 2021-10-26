from typing import List
from airflow.kubernetes.secret import Secret


class DbtExecutionEnvironmentParameters:
    def __init__(self, target: str, project_dir_path: str, profile_dir_path: str):
        self.target = target
        self.project_dir_path = project_dir_path
        self.profile_dir_path = profile_dir_path


class KubernetesExecutionParameters:
    def __init__(
        self,
        namespace: str,
        image: str,
        node_selectors: dict = None,
        tolerations: list = None,
        labels: dict = None,
        resources=None,
        secrets: List[Secret] = None,
        is_delete_operator_pod: bool = True,
    ):
        self.namespace = namespace
        self.image = image
        self.node_selectors = node_selectors
        self.tolerations = tolerations
        self.labels = labels
        self.resources = resources
        self.secrets = secrets
        self.is_delete_operator_pod = is_delete_operator_pod
