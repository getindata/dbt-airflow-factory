"""POD representing Kubernetes operator config file."""

from typing import Any


class EcsExecutionParameters:
    """
    :param image: tag of Docker image you wish to launch.
    :type image: str
    """

    def __init__(
        self,
        image: str,
        **_kwargs: Any,
    ) -> None:
        self.image = image
