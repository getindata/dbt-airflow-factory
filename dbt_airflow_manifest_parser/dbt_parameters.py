class DbtExecutionEnvironmentParameters:
    def __init__(
        self,
        target: str,
        project_dir_path: str = "/dbt",
        profile_dir_path: str = "/root/.dbt",
    ):
        self.target = target
        self.project_dir_path = project_dir_path
        self.profile_dir_path = profile_dir_path
