"""POD representing DBT operator config file."""

import sys
from typing import Any, Dict, Optional

import yaml


class DbtExecutionEnvironmentParameters:
    """POD representing DBT operator config file.

    :param target: Name of the target environment (passed to **dbt** as ``--target``).
    :type target: str
    :param project_dir_path: Path to project directory.
    :type project_dir_path: str
    :param profile_dir_path: Path to the directory containing ``profiles.yml``.
    :type project_dir_path: str
    :param vars: Dictionary of variables to pass to the **dbt**.
    :type vars: Optional[Dict[str, str]]
    """

    def __init__(
        self,
        target: str,
        project_dir_path: str = "/dbt",
        profile_dir_path: str = "/root/.dbt",
        vars: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> None:
        self.target = target
        self.project_dir_path = project_dir_path
        self.profile_dir_path = profile_dir_path
        self._vars = vars or {}

    @property
    def vars(self) -> str:
        """
        String representation of dictionary of **dbt** variables.

        DBT expects ``--vars`` passing string in YAML format. This property
        returns such a string.

        :return: String representation of dictionary of **dbt** variables.
        :rtype: str
        """
        return yaml.dump(self._vars, default_flow_style=True, width=sys.maxsize).rstrip()
