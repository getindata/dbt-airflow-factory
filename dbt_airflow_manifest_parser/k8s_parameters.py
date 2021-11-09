from typing import List

import airflow

if airflow.__version__.startswith("1."):
    from airflow.contrib.kubernetes.secret import Secret
else:
    from airflow.kubernetes.secret import Secret


class KubernetesExecutionParameters:
    def __init__(
        self,
        image: str,
        namespace: str = "default",
        image_pull_policy: str = None,
        node_selectors: dict = None,
        tolerations: list = None,
        labels: dict = None,
        limit: dict = None,
        requests: dict = None,
        annotations: dict = None,
        secrets: List[Secret] = None,
        is_delete_operator_pod: bool = True,
    ):
        self.namespace = namespace
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.node_selectors = node_selectors
        self.tolerations = tolerations
        self.labels = labels
        self.limit = limit
        self.requests = requests
        self.annotations = annotations
        self.secrets = secrets
        self.is_delete_operator_pod = is_delete_operator_pod

    def get_resources(self):
        if airflow.__version__.startswith("1."):
            return {
                "limit_memory": self.limit["memory"],
                "limit_cpu": self.limit["cpu"],
                "request_memory": self.requests["memory"],
                "request_cpu": self.requests["cpu"],
            }
        else:
            from kubernetes.client import models as k8s

            return k8s.V1ResourceRequirements(limits=self.limit, requests=self.requests)
