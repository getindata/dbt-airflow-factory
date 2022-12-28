"""POD representing Kubernetes operator config file."""

from typing import Any, Dict, List, Optional




class BashExecutionParameters:
    """POD representing Kubernetes operator config file.

    :param image: tag of Docker image you wish to launch.
    :type image: str
    :param namespace: the namespace to run within Kubernetes.
    :type namespace: str
    :param image_pull_policy: Specify a policy to cache or always pull an image.
    :type image_pull_policy: str
    :param node_selectors: A dict containing a group of scheduling rules.
    :type node_selectors: dict
    :param tolerations: A list of Kubernetes tolerations.
    :type tolerations: list
    :param labels: labels to apply to the Pod. (templated)
    :type labels: dict
    :param limit: A dict containing resources limits.
    :type limit: dict
    :param requests: A dict containing resources requests.
    :type requests: dict
    :param annotations: non-identifying metadata you can attach to the Pod.
    :type annotations: dict
    :param envs: Environment variables initialized in the container. (templated)
    :type envs: Optional[Dict[str, str]]
    :param secrets: Kubernetes secrets to inject in the container.
        They can be exposed as environment vars or files in a volume.
    :type secrets: List[Secret]
    :param is_delete_operator_pod: What to do when the pod reaches its final
        state, or the execution is interrupted. If True: delete the pod.
    :type is_delete_operator_pod: bool
    :param execution_script: Script that will be executed inside pod.
    :type execution_script: str
    :param in_cluster: Run kubernetes client with in_cluster configuration.
    :type in_cluster: bool
    :param cluster_context: Context that points to kubernetes cluster.
        Ignored when in_cluster is True. If None, current-context is used.
    :type cluster_context: str
    """

    def __init__(
        self,
        execution_script: str = "dbt --no-write-json",
        **kwargs: Any,
    ) -> None:
        self.execution_script = execution_script

