from typing import List

import airflow
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
        limit_resources: dict = None,
        requested_resourses: dict = None,
        annotations: dict = None,
        secrets: List[Secret] = None,
        is_delete_operator_pod: bool = True,
    ):
        self.namespace = namespace
        self.image = image
        self.node_selectors = node_selectors
        self.tolerations = tolerations
        self.labels = labels
        self.limit_resources = limit_resources
        self.requested_resourses = requested_resourses
        self.annotations = annotations
        self.secrets = secrets
        self.is_delete_operator_pod = is_delete_operator_pod

    def get_resources(self):
        if airflow.__version__.startswith("1."):
            return {
                "limit_memory": self.limit_resources["memory"],
                "limit_cpu": self.limit_resources["cpu"],
                "requests_memory": self.requested_resourses["memory"],
                "requests_cpu": self.requested_resourses["memory"],
            }
        else:
            from kubernetes.client import models as k8s

            return k8s.V1ResourceRequirements(
                limits=self.limit_resources, requests=self.requested_resourses
            )


# List[k8s.V1Toleration]:
# """
# Creates k8s tolerations
# :param tolerations:
# :return:
# """
# if not tolerations:
#     return []
# return [
#     k8s.V1Toleration(
#         effect=toleration.get("effect"),
#         key=toleration.get("key"),
#         operator=toleration.get("operator"),
#         value=toleration.get("value"),
#     )
#     for toleration in tolerations
# ]
