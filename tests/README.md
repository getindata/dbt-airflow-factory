# dbt-airflow-factory Tests

This directory contains tests for the dbt-airflow-factory package.

## Test Structure

- `test_dag_factory.py` - Tests for AirflowDagFactory and Cosmos integration
- `test_notifications.py` - Tests for notification handlers (Slack, MS Teams)
- `test_cosmos_*.py` - Unit tests for Cosmos configuration builders

## Fixtures

The `fixtures/dbt_project/` directory contains a minimal dbt project used for testing:

- `dbt_project.yml` - dbt project configuration
- `manifest.json` - Compiled dbt manifest with test models
- `models/` - Placeholder SQL files for dbt models

These fixtures are used by Cosmos to validate dbt project structure during DAG creation.

### Updating Fixtures

If you need to add new models or change the dbt project structure for tests:

1. Edit `fixtures/dbt_project/manifest.json` to add/modify model metadata
2. Add corresponding `.sql` files in `fixtures/dbt_project/models/`
3. Update `fixtures/dbt_project/dbt_project.yml` if configuration changes are needed

## Running Tests

### Local Testing

```bash
# Install with test dependencies
pip install -e ".[tests]"

# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=dbt_airflow_factory --cov-report=term-missing

# Run specific test file
pytest tests/test_dag_factory.py -v
```

### Testing Across Python Versions

```bash
# Test all supported Python versions
tox

# Test specific Python version
tox -e py39
tox -e py310
tox -e py311
```

## Supported Python Versions

- Python 3.9
- Python 3.10
- Python 3.11

Python 3.8 and 3.12 are not currently supported.

## Configuration

Test configurations are stored in `config/` subdirectories:

- `config/base/` - Base configuration shared across all tests
- `config/dev/`, `config/gateway/`, etc. - Environment-specific overrides

**Note:** The `manifest_file_name` config option is deprecated with Cosmos integration. Cosmos now reads the manifest directly from the `project_dir_path` specified in `dbt.yml`.
