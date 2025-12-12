# Changelog

## [Unreleased]

### Changed

-   **BREAKING:** Dropped Airflow 1.x support (min: `apache-airflow>=2.5`)
-   **BREAKING:** Dropped Python 3.8 (min: Python 3.9)
-   **MAJOR:** Replaced `dbt-graph-builder` dependency with `astronomer-cosmos>=1.10.0, <2.0`
-   **MAJOR:** Removed custom task builders (~900 lines total): `tasks_builder/`, `k8s/`, `ecs/`, `bash/`, `operator.py`, `dbt_parameters.py`, `builder_factory.py`, `tasks.py`
-   Rewrote `AirflowDagFactory` to use Cosmos `DbtTaskGroup` for dbt task generation
-   Replaced `apache-airflow[kubernetes,slack]` with individual provider packages (no upper bounds)
-   Updated `apache-airflow-providers-airbyte` to remove upper bound (auto-resolves based on Airflow version)
-   Migrated `DummyOperator` → `EmptyOperator` (Airflow 2.4+ standard)

### Added

-   Python 3.12 support

-   New `cosmos/` module for Cosmos integration (~400 lines):
    -   `project_config_builder.py` - Maps `dbt.yml` to Cosmos `ProjectConfig`
    -   `profile_config_builder.py` - Maps `profile_dir_path` to Cosmos `ProfileConfig`
    -   `execution_config_builder.py` - Maps `execution_env.yml` to Cosmos `ExecutionConfig`
    -   `operator_args_builder.py` - Transparent pass-through of `k8s.yml` config to Cosmos
    -   `config_translator.py` - Main translator coordinating all config builders
-   Optional `cosmos.yml` configuration file support for advanced Cosmos features

### Breaking Changes (Internal Only)

-   **Dependency Change:** `dbt-graph-builder` removed, `astronomer-cosmos` added
-   **Removed Modules:** All custom task builder code deleted (internal implementation)
-   **ECS Execution Removed:** `type: ecs` in `execution_env.yml` is no longer supported (Cosmos 1.10 limitation). Use `type: k8s` or `type: docker` instead
-   **Behavior Change:** `use_task_group: false` config now ignored (Cosmos always uses `DbtTaskGroup`)
-   **Behavior Change:** `seed_task` config now ignored (Cosmos automatically creates seed tasks from manifest)

### Tests

-   Python 3.9, 3.10, 3.11 supported
-   Rewrote tests for Cosmos (50 passing)
-   Added `tests/fixtures/` with manifest files for various test scenarios

### Backward Compatibility Maintained

-   ✅ **Zero config changes required:** All existing YAML files work unchanged
-   ✅ **Zero code changes required:** Same API - `AirflowDagFactory(dag_path, env).create()`
-   ✅ **Preserved features:** Airbyte ingestion, Slack/Teams notifications, DataHub integration
-   ✅ **Migration:** Only requires `pip install --upgrade dbt-airflow-factory`

### Improvements

-   Support for Airflow 2.5-2.11 (previously limited to 2.x)
-   Support for dbt 1.7-1.10 via Cosmos (peer dependency)
-   Reduced codebase by ~600 net lines
-   Simplified architecture: delegate dbt execution to Cosmos, focus on orchestration
-   Model-level task visibility in Airflow UI (Cosmos feature)
-   More execution modes available: Kubernetes, Local, Docker, VirtualEnv

## [0.36.0] - 2025-12-01

-   Add `Google Chat` notifications handler

## [0.35.0] - 2023-09-08

## [0.34.0] - 2023-08-10

-   Add `MS Teams` notifications handler

## [0.33.0] - 2023-08-04

-   Add `kwargs` to `BashExecutionParameters` [#90](https://github.com/getindata/dbt-airflow-factory/issues/90)
-   Correcting required packages [#97](https://github.com/getindata/dbt-airflow-factory/issues/97)

## [0.32.0] - 2023-07-04

## [0.31.1] - 2023-05-12

### Fixed

-   Replace `config_file` default value from `"~/.kube/config"` to `None` in `KubernetesPodOperator` [#90](https://github.com/getindata/dbt-airflow-factory/issues/90)

## [0.31.0] - 2023-03-27

### Fixed

-   Use `node_selector` and `container_resources` parameters in `KubernetesPodOperator` if Airflow is 2.3+.

## [0.30.0] - 2023-02-08

-   Add in_cluster, cluster_context params
-   Repair secrets to be not required
-   Update docs
-   Add BashOperator
-   Exposes param to control the pod startup timeout

## [0.29.0] - 2022-09-02

## [0.28.0] - 2022-07-19

## [0.27.0] - 2022-07-01

## [0.26.0] - 2022-05-13

-   Documentation improvements

## [0.25.0] - 2022-04-27

## [0.24.0] - 2022-04-22

-   Dependencies between project in Airflow

## [0.23.0] - 2022-03-22

## [0.22.0] - 2022-03-21

-   Failure notifications via slack

### Added

-   Ephemeral nodes can be hidden from DAG by setting `show_ephemeral_models: False` in project's `airflow.yml`.

## [0.21.0] - 2022-02-11

This version brings compatibility with `dbt 1.0`.

## [0.20.1] - 2022-02-08

## [0.20.0] - 2022-02-08

### Added

-   Run tests with more than one dependency in a different node.

## [0.19.0] - 2022-02-02

## [0.18.1] - 2022-01-18

### Fixed

-   Jinja's `FileSystemLoader` gets `str` instead of `pathlib.Path` to fix types incompatibility for `Jinja < 2.11.0`.
-   Use `get_start_task()` and `get_end_task()` in `AirflowDagFactory.create_tasks(config)` to prevent ephemeral ending tasks from throwing.

## [0.18.0] - 2022-01-14

### Added

-   ReadTheDocs documentation in `docs` directory.
-   `{{ var.value.VARIABLE_NAME }}` gets replaced with Airflow variables when parsing `airflow.yml` file.

### Changed

-   Rename project from `dbt-airflow-manifest-parser` to `dbt-airflow-factory`.

### Fixed

-   `KubernetesExecutionParameters.env_vars` works in Airflow 1 too. Airflow 1 is expecting a real dictionary of
    environment variables instead of a list of `k8s.V1EnvVar` objects.
-   Fix `DummyOperator` import in `operator.py` to work in Airflow 1.

## [0.17.0] - 2022-01-11

### Changed

-   Ephemeral models are not run anymore, presented as an `EphemeralOperator` deriving from the `DummyOperator`.

## [0.16.0] - 2022-01-05

### Added

-   Add support for `vars` in `dbt.yml`.

## [0.15.0] - 2021-12-13

### Changed

-   Drop `<TASK_ID>_` prefix from Task names when using TaskGroup.

## [0.14.0] - 2021-12-06

### Changed

-   Add `**kwargs` argument to `DbtExecutionEnvironmentParameters` and `KubernetesExecutionParameters` constructors,
    making them ignore additional arguments, if provided.
-   Add support for Kubernetes environment variables.

## [0.13.0] - 2021-11-17

-   Allow usage of TaskGroup when `use_task_group` flag is set to `True`

## [0.12.0] - 2021-11-17

## [0.11.0] - 2021-11-10

## [0.10.0] - 2021-11-09

## [0.9.0] - 2021-11-05

## [0.8.0] - 2021-11-03

## [0.7.0] - 2021-11-02

## [0.6.0] - 2021-11-02

## [0.5.0] - 2021-10-29

-   Automatic parsing config files

## [0.4.0] - 2021-10-27

## [0.3.0] - 2021-10-27

-   Support for Airflow 2.x

## [0.2.0] - 2021-10-25

-   Initial implementation of `dbt_airflow_manifest_parser` library.

[Unreleased]: https://github.com/getindata/dbt-airflow-factory/compare/0.36.0...HEAD

[0.36.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.35.0...0.36.0

[0.35.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.34.0...0.35.0

[0.34.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.33.0...0.34.0

[0.33.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.32.0...0.33.0

[0.32.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.31.1...0.32.0

[0.31.1]: https://github.com/getindata/dbt-airflow-factory/compare/0.31.0...0.31.1

[0.31.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.30.0...0.31.0

[0.30.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.29.0...0.30.0

[0.29.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.28.0...0.29.0

[0.28.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.27.0...0.28.0

[0.27.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.26.0...0.27.0

[0.26.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.25.0...0.26.0

[0.25.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.24.0...0.25.0

[0.24.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.23.0...0.24.0

[0.23.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.22.0...0.23.0

[0.22.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.21.0...0.22.0

[0.21.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.20.1...0.21.0

[0.20.1]: https://github.com/getindata/dbt-airflow-factory/compare/0.20.0...0.20.1

[0.20.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.19.0...0.20.0

[0.19.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.18.1...0.19.0

[0.18.1]: https://github.com/getindata/dbt-airflow-factory/compare/0.18.0...0.18.1

[0.18.0]: https://github.com/getindata/dbt-airflow-factory/compare/0.17.0...0.18.0

[0.17.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.16.0...0.17.0

[0.16.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.15.0...0.16.0

[0.15.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.14.0...0.15.0

[0.14.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.13.0...0.14.0

[0.13.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.12.0...0.13.0

[0.12.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.11.0...0.12.0

[0.11.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.10.0...0.11.0

[0.10.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.9.0...0.10.0

[0.9.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.8.0...0.9.0

[0.8.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.7.0...0.8.0

[0.7.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.6.0...0.7.0

[0.6.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.5.0...0.6.0

[0.5.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.4.0...0.5.0

[0.4.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.3.0...0.4.0

[0.3.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.2.0...0.3.0

[0.2.0]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/6395f7ea175caa3bd1aca361e9d2f7fb7f7a7820...0.2.0
