# Test Suite Capability

## Purpose

Validates that the dbt-airflow-factory library correctly integrates with Astronomer Cosmos and generates valid Airflow DAGs across supported Python versions.

## Requirements

### Requirement: Test suite validates Cosmos-based DAG generation
The test suite MUST validate that Cosmos-based DAG generation works correctly across all supported Python versions.

#### Scenario: Running full test suite passes all tests
**Given** the codebase uses Astronomer Cosmos for DAG generation
**When** a developer runs `pytest tests/`
**Then** all tests pass successfully
**And** test coverage is above 80%
**And** no deprecation warnings are shown

#### Scenario: Tests validate DAG factory with real dbt projects
**Given** Cosmos validates dbt project files exist
**When** `test_dag_factory.py` tests run
**Then** tests use valid dbt project fixtures in `tests/fixtures/dbt_project/`
**And** fixtures include `dbt_project.yml` and `manifest.json`
**And** all 11 dag_factory tests pass

#### Scenario: Notification tests use correct mock assertions
**Given** Python 3.11+ enforces strict mock assertion syntax
**When** `test_notifications.py` tests run
**Then** mock assertions use proper methods like `assert_called_once()`
**And** all 3 notification tests pass

### Requirement: Test suite supports Python 3.9 through 3.11
The test suite MUST pass on Python 3.9, 3.10, and 3.11. Python 3.8 and 3.12 SHALL NOT be tested.

#### Scenario: Running tox tests across Python versions
**Given** the project supports Python 3.9, 3.10, and 3.11
**When** a developer runs `tox`
**Then** tests pass on Python 3.9
**And** tests pass on Python 3.10
**And** tests pass on Python 3.11
**And** Python 3.8 and 3.12 are not tested

