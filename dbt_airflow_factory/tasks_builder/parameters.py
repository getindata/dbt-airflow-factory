from dataclasses import dataclass


@dataclass(frozen=True)
class TasksBuildingParameters:
    use_task_group: bool = True
    show_ephemeral_models: bool = True
    enable_dags_dependencies: bool = False
    check_all_deps_for_multiple_deps_tests: bool = False
