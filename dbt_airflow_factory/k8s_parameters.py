"""POD representing Kubernetes operator config file."""

from typing import Any, Dict, List, Optional

import airflow

if airflow.__version__.startswith("1."):
    from airflow.contrib.kubernetes.secret import Secret
else:
    from airflow.kubernetes.secret import Secret


class KubernetesExecutionParameters:
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
    """

    def __init__(
        self,
        image: str,
        namespace: str = "default",
        image_pull_policy: Optional[str] = None,
        node_selectors: Optional[dict] = None,
        tolerations: Optional[list] = None,
        labels: Optional[dict] = None,
        limit: Optional[dict] = None,
        requests: Optional[dict] = None,
        annotations: Optional[dict] = None,
        envs: Optional[Dict[str, str]] = None,
        secrets: Optional[List[Secret]] = None,
        is_delete_operator_pod: bool = True,
        **kwargs: Any,
    ) -> None:
        self.namespace = namespace
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.node_selectors = node_selectors
        self.tolerations = tolerations
        self.labels = labels
        self._limit = limit
        self._requests = requests
        self.annotations = annotations
        self._env_vars = envs
        self.secrets = secrets
        self.is_delete_operator_pod = is_delete_operator_pod

    @property
    def resources(self):  # type: ignore
        """
        Return dict containing resources requests and limits.

        In the Airflow 1, it was expected to be a real dictionary with
        ``request_memory``, ``request_cpu``, ``limit_memory``, and ``limit_cpu``
        as keys. So for Airflow 1, the function returns such a dictionary.

        Beginning with Airflow 2, :class:`KubernetesPodOperator` expects
        ``V1ResourceRequirements`` class instead. Hence, for Airflow 2, the
        function returns instance of this class.

        :return: Dictionary containing resources requests and limits.
        """
        if airflow.__version__.startswith("1."):
            return {
                "limit_memory": self._limit["memory"] if self._limit else None,
                "limit_cpu": self._limit["cpu"] if self._limit else None,
                "request_memory": self._requests["memory"] if self._requests else None,
                "request_cpu": self._requests["cpu"] if self._requests else None,
            }
        else:
            from kubernetes.client import models as k8s

            return k8s.V1ResourceRequirements(
                limits=self._limit, requests=self._requests
            )

    @property
    def env_vars(self):  # type: ignore
        """
        Return dict containing environment variables to set in Kubernetes.

        For Airflow 1, the function returns a dictionary.

        Beginning with Airflow 2, :class:`KubernetesPodOperator` expects
        a list of ``V1EnvVar`` instances instead. Hence, for Airflow 2, the
        function returns a ``List[k8s.V1EnvVar]``.

        :return: Dictionary or list containing environment variables.
        """
        if self._env_vars is None:
            return None

        if airflow.__version__.startswith("1."):
            return self._env_vars
        else:
            from kubernetes.client import models as k8s

            return [k8s.V1EnvVar(k, v) for k, v in self._env_vars.items()]
