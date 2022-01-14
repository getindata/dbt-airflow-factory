# Changelog

## [Unreleased]

### Added
- ReadTheDocs documentation in `docs` directory.

### Changed
- Rename project from `dbt-airflow-manifest-parser` to `dbt-airflow-factory`.

### Fixed
- `KubernetesExecutionParameters.env_vars` works in Airflow 1 too. Airflow 1 is expecting a real dictionary of
  environment variables instead of a list of `k8s.V1EnvVar` objects.

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

[Unreleased]: https://github.com/getindata/dbt-airflow-manifest-parser/compare/0.17.0...HEAD

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
