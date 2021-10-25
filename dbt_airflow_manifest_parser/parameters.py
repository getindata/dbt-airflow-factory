class DbtExecutionEnvironmentParameters:
    def __init__(self, target, project_dir_path, profile_dir_path):
        self.target = target
        self.project_dir_path = project_dir_path
        self.profile_dir_path = profile_dir_path


class KubernetesExecutionParameters:
    def __init__(
        self,
        namespace,
        image,
        node_selectors=None,
        tolerations=None,
        labels=None,
        resources=None,
        secrets=None,
        is_delete_operator_pod=True,
    ):
        self.namespace = namespace
        self.image = image
        self.node_selectors = node_selectors
        self.tolerations = tolerations
        self.labels = labels
        self.resources = resources
        self.secrets = secrets
        self.is_delete_operator_pod = is_delete_operator_pod
