import sys
from typing import Dict, Optional

import yaml


class DbtExecutionEnvironmentParameters:
    def __init__(
        self,
        target: str,
        project_dir_path: str = "/dbt",
        profile_dir_path: str = "/root/.dbt",
        vars: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        self.target = target
        self.project_dir_path = project_dir_path
        self.profile_dir_path = profile_dir_path
        self._vars = vars or {}

    @property
    def vars(self) -> str:
        return yaml.dump(
            self._vars, default_flow_style=True, width=sys.maxsize
        ).rstrip()
