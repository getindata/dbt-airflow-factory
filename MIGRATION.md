# Migration Guide: v0.35.0 → v1.0.0

This guide helps you migrate from dbt-airflow-factory v0.35.0 (using dbt-graph-builder) to v1.0.0 (using Astronomer Cosmos).

## Overview

Version 1.0.0 is a major internal refactoring that replaces the custom task builder system with Astronomer Cosmos integration. This change:

- ✅ **Reduces codebase by ~600 lines** (~1000 lines removed, ~400 added)
- ✅ **Maintains 100% backward compatibility** with existing configurations
- ✅ **Requires zero configuration changes** to your YAML files
- ✅ **Requires zero code changes** to your DAG definitions
- ✅ **Preserves all existing features** (Airbyte, notifications, DataHub)
- ✅ **Supports same Airflow/dbt versions** (Airflow 2.5-2.11, dbt 1.7-1.10)

## What's Changed Internally

### Replaced Components

| Old (v0.35.0) | New (v1.0.0) | Purpose |
|---------------|--------------|---------|
| `dbt_graph_builder` | `astronomer-cosmos>=1.11.0` | Dependency |
| Custom task builders | Cosmos `DbtTaskGroup` | Task orchestration |
| `builder_factory.py` | `cosmos/config_translator.py` | Configuration mapping |
| `tasks_builder/` | `cosmos/` module | Core logic |
| `operator.py` | Cosmos operators | Task execution |

### Removed Modules

The following modules were removed as Cosmos handles their functionality:

- `dbt_airflow_factory/tasks_builder/` (entire directory)
- `dbt_airflow_factory/builder_factory.py`
- `dbt_airflow_factory/operator.py`
- `dbt_airflow_factory/dbt_parameters.py`
- `dbt_airflow_factory/k8s/`
- `dbt_airflow_factory/ecs/`
- `dbt_airflow_factory/bash/`
- `dbt_airflow_factory/tasks.py`

**Action required:** If you have unused imports from these modules in your DAG files, remove them:

```python
# Remove these imports (no longer available):
from dbt_airflow_factory.dbt_parameters import DbtExecutionEnvironmentParameters
from dbt_airflow_factory.k8s.k8s_operator import KubernetesPodOperatorBuilder
from dbt_airflow_factory.k8s.k8s_parameters_loader import KubernetesExecutionParametersLoader
```

### Added Modules

New `cosmos/` module provides clean configuration translation:

- `cosmos/config_translator.py` - Main coordinator
- `cosmos/project_config_builder.py` - Maps `dbt.yml` to Cosmos `ProjectConfig`
- `cosmos/profile_config_builder.py` - Maps `dbt.yml` to Cosmos `ProfileConfig`
- `cosmos/execution_config_builder.py` - Maps `execution_env.yml` to Cosmos `ExecutionConfig`
- `cosmos/operator_args_builder.py` - Handles k8s/DataHub/Cosmos config merging

## What's NOT Changed

### Your Configurations (Minimal Changes)

All your existing YAML configurations work without modification, with two new optional fields:

- ✅ `config/base/dbt.yml` - Same format, NEW: optional `dbt_project_name` field
- ✅ `config/base/airflow.yml` - Same format, NEW: optional `manifest_file_name` field
- ✅ `config/base/execution_env.yml` - Same format, same fields
- ✅ `config/base/k8s.yml` - Same format, same fields (transparent pass-through)
- ✅ `config/{env}/dbt.yml` - Same override pattern
- ✅ `config/{env}/datahub.yml` - Same format, same fields
- ✅ `config/{env}/<target_type>.yml` (e.g., `snowflake.yml`) - Same format, same fields

**New optional fields:**
```yaml
# config/base/airflow.yml
manifest_file_name: dbt_project/target/manifest.json  # Default: manifest.json

# config/base/dbt.yml
dbt_project_name: my_project  # Default: auto-extracted from manifest
```

### Your DAG Code (Zero Changes Required)

Your DAG definitions continue to work as-is:

```python
from dbt_airflow_factory import DbtAirflowFactory

# This code remains unchanged from v0.35.0 → v1.0.0
with DbtAirflowFactory(
    dag_id="my_dbt_dag",
    dag_path="/path/to/dag",
    environment="production",
) as dag_factory:
    dag = dag_factory.create()
```

### Your Features

All existing features continue to work:

- ✅ Airbyte integration (via Airflow sensors)
- ✅ Notification handling (Slack, email)
- ✅ DataHub integration (environment variable injection)
- ✅ Multi-environment support (dev, staging, production)
- ✅ Custom execution environments (k8s, bash, docker)

## Migration Steps

### Step 1: Update Dependencies

Update your `requirements.txt` or `pyproject.toml`:

```diff
- dbt-airflow-factory==0.35.0
+ dbt-airflow-factory==1.0.0
```

Then reinstall:

```bash
pip install --upgrade dbt-airflow-factory
```

### Step 2: Verify Your Configuration Files

**No changes needed**, but verify these files exist and follow the documented format:

1. **Required files:**
   - `config/base/dbt.yml` - dbt project configuration
   - `config/base/execution_env.yml` - execution environment type
   - `config/base/k8s.yml` (if using k8s) - Kubernetes configuration

2. **Optional files:**
   - `config/{env}/dbt.yml` - environment-specific dbt overrides
   - `config/{env}/datahub.yml` - DataHub integration
   - `config/{env}/cosmos.yml` - Cosmos-specific overrides (new in v1.0.0)
   - `config/{env}/<target_type>.yml` - dbt profile connection details

### Step 3: Test Your DAGs

1. **Dry-run test:**
   ```bash
   airflow dags test my_dbt_dag 2024-01-01
   ```

2. **Verify task structure:**
   ```bash
   airflow tasks list my_dbt_dag
   ```

   You should see the same dbt tasks as before, now orchestrated by Cosmos.

3. **Run a backfill:**
   ```bash
   airflow dags backfill my_dbt_dag --start-date 2024-01-01 --end-date 2024-01-01
   ```

### Step 4: Monitor First Production Run

On the first production run, monitor:

- ✅ Tasks execute in correct order (same as v0.35.0)
- ✅ Environment variables are injected correctly (especially DataHub vars)
- ✅ Kubernetes pods use correct namespace/resources (if using k8s)
- ✅ Notifications fire correctly

## Troubleshooting

### Issue: "No module named 'dbt_graph_builder'"

**Cause:** Old import statements or cached dependencies

**Fix:**
```bash
pip uninstall dbt-graph-builder -y
pip install --force-reinstall dbt-airflow-factory==1.0.0
```

### Issue: "ExecutionConfig.__init__() got an unexpected keyword argument"

**Cause:** Using removed internal APIs directly (if you extended the factory)

**Fix:** The internal API has changed. Review the new `cosmos/` module structure or contact maintainers for guidance on custom extensions.

### Issue: Tasks not running in expected order

**Cause:** Cosmos may interpret dbt dependencies differently

**Fix:**
1. Verify your `manifest.json` is up to date: `dbt compile`
2. Check task dependencies in Airflow UI
3. Set `load_mode` in `config/{env}/cosmos.yml` if using custom manifest location:
   ```yaml
   # config/{env}/cosmos.yml
   load_mode: dbt_manifest
   ```

### Issue: Environment variables not propagating to dbt

**Cause:** `envs` configuration format mismatch

**Fix:** Verify your `config/base/k8s.yml` uses dictionary format:
```yaml
# Correct format:
envs:
  MY_VAR: "value"
  OTHER_VAR: "other"

# Incorrect format (will fail):
envs:
  - name: MY_VAR
    value: "value"
```

### Issue: DataHub integration not working

**Cause:** `datahub.yml` environment variables not merging correctly

**Fix:** Verify `config/{env}/datahub.yml` structure:
```yaml
# config/{env}/datahub.yml
datahub_gms_url: "http://datahub-gms:8080"
datahub_env_vars:
  DATAHUB_GMS_URL: "http://datahub-gms:8080"
  DATAHUB_ENV: "PROD"
```

The `datahub_env_vars` dictionary will be merged into the `envs` section automatically.

### Issue: Kubernetes resources not applied

**Cause:** `resources` configuration format issue

**Fix:** Verify your `config/base/k8s.yml` resources structure matches Cosmos expectations:
```yaml
# config/base/k8s.yml
resources:
  node_selectors:
    group: data-processing
  tolerations:
    - key: group
      operator: Equal
      value: data-processing
      effect: NoSchedule
  limit:
    memory: "2048M"
    cpu: "2"
  requests:
    memory: "1024M"
    cpu: "1"
```

This entire structure is passed through transparently to Cosmos.

## New Optional Features (v1.0.0)

### cosmos.yml - Cosmos-Specific Overrides

You can now optionally create `config/{env}/cosmos.yml` to override Cosmos-specific settings:

```yaml
# config/{env}/cosmos.yml

# Override load mode (default: automatic detection)
load_mode: dbt_manifest

# Override operator_args for all tasks
operator_args:
  install_deps: true
  full_refresh: false

  # Override k8s settings from k8s.yml
  image_pull_policy: "Always"

  # Add/override environment variables
  envs:
    COSMOS_SPECIFIC_VAR: "value"
```

**Common use cases:**

1. **Force manifest mode:**
   ```yaml
   load_mode: dbt_manifest
   ```

2. **Always install dependencies:**
   ```yaml
   operator_args:
     install_deps: true
   ```

3. **Environment-specific overrides:**
   ```yaml
   # config/production/cosmos.yml
   operator_args:
     full_refresh: false

   # config/development/cosmos.yml
   operator_args:
     full_refresh: true
   ```

### Docker Image Configuration

Docker image configuration from `config/base/execution_env.yml` is now automatically converted to Cosmos operator_args format:

```yaml
# config/base/execution_env.yml
type: k8s
image:
  repository: gcr.io/my-project/dbt
  tag: "1.7.0"
```

This automatically becomes `operator_args["image"] = "gcr.io/my-project/dbt:1.7.0"` in Cosmos.

## Deprecated Configuration: execution_script

### Deprecation Notice

The `execution_script` field in `config/base/execution_env.yml` is **deprecated** in v1.0.0 and will be removed in a future version. It has been replaced by Cosmos-native dbt configuration parameters.

**Old configuration (deprecated):**
```yaml
# config/base/execution_env.yml
type: k8s
execution_script: "/usr/local/bin/custom-dbt"
```

**New configuration (recommended):**
```yaml
# config/{env}/cosmos.yml
operator_args:
  dbt_executable_path: "/usr/local/bin/custom-dbt"
```

### Why This Changed

Cosmos provides more granular control over dbt execution through three parameters:
- `dbt_executable_path` - Path to the dbt executable (replaces `execution_script`)
- `dbt_cmd_global_flags` - Flags applied globally to all dbt commands (e.g., `--no-write-json`, `--debug`)
- `dbt_cmd_flags` - Flags applied to specific dbt commands (e.g., `--full-refresh`)

### Migration Path

**Option 1: Let it auto-migrate (with deprecation warning)**

Your existing `execution_script` configuration will continue to work, but you'll see a deprecation warning in logs:

```
DeprecationWarning: 'execution_script' is deprecated. Use 'dbt_executable_path' in operator_args.
```

**Option 2: Migrate to cosmos.yml (recommended)**

Create or update `config/{env}/cosmos.yml`:

```yaml
# config/production/cosmos.yml
operator_args:
  # Simple case: just the executable path
  dbt_executable_path: "/usr/local/bin/custom-dbt"

  # Advanced case: with additional flags
  dbt_cmd_global_flags:
    - "--no-write-json"
    - "--debug"
  dbt_cmd_flags:
    - "--full-refresh"
```

**Option 3: Override per environment**

```yaml
# config/development/cosmos.yml
operator_args:
  dbt_executable_path: "/usr/local/bin/dbt"
  dbt_cmd_flags:
    - "--full-refresh"  # Always full refresh in dev

# config/production/cosmos.yml
operator_args:
  dbt_executable_path: "/usr/local/bin/dbt"
  dbt_cmd_flags: []  # Incremental builds in prod
```

### Precedence Rules

If you specify both old and new configurations, **cosmos.yml takes precedence**:

```yaml
# config/base/execution_env.yml
execution_script: "/old/path/dbt"  # Will be ignored

# config/production/cosmos.yml
operator_args:
  dbt_executable_path: "/new/path/dbt"  # This wins
```

### Available Cosmos dbt Parameters

All Cosmos `operator_args` are now supported. Common parameters:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `dbt_executable_path` | Path to dbt executable | `/usr/local/bin/dbt` |
| `dbt_cmd_global_flags` | Flags for all dbt commands | `["--no-write-json"]` |
| `dbt_cmd_flags` | Flags for specific commands | `["--full-refresh"]` |
| `install_deps` | Run `dbt deps` before execution | `true` |
| `full_refresh` | Force full refresh | `false` |

See [Cosmos documentation](https://astronomer-cosmos.readthedocs.io) for the complete list.

## Advanced: Extending DbtAirflowFactory

If you've extended `DbtAirflowFactory` in your codebase, review the new internal structure:

### Old Architecture (v0.35.0)
```python
# Old way - no longer works
from dbt_airflow_factory.builder_factory import get_dbt_task_builder

builder = get_dbt_task_builder(
    execution_env="k8s",
    dbt_params=dbt_params,
    k8s_params=k8s_params,
)
tasks = builder.build_tasks()
```

### New Architecture (v1.0.0)
```python
# New way
from dbt_airflow_factory.cosmos.config_translator import translate_configs
from cosmos import DbtTaskGroup

project_config, profile_config, execution_config, operator_args = translate_configs(
    dbt_config=dbt_config,
    execution_env_config=execution_env_config,
    k8s_config=k8s_config,
    datahub_config=datahub_config,
    cosmos_config=cosmos_config,
)

dbt_task_group = DbtTaskGroup(
    group_id="dbt",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args=operator_args,
)
```

## Configuration Directory Structure

For reference, here's the complete configuration directory structure:

```
your-dag/
├── config/
│   ├── base/                          # Base configuration (shared across environments)
│   │   ├── airflow.yml                # Airflow DAG settings
│   │   ├── dbt.yml                    # dbt project settings
│   │   ├── execution_env.yml          # Execution environment (k8s/bash/docker)
│   │   └── k8s.yml                    # Kubernetes configuration (if using k8s)
│   │
│   ├── dev/                           # Development environment overrides
│   │   ├── dbt.yml                    # Override target, vars, etc.
│   │   ├── datahub.yml                # DataHub integration (optional)
│   │   ├── cosmos.yml                 # Cosmos overrides (optional, new in v1.0.0)
│   │   └── snowflake.yml              # dbt profile connection details
│   │
│   ├── staging/                       # Staging environment overrides
│   │   └── ...                        # Same structure as dev/
│   │
│   └── production/                    # Production environment overrides
│       └── ...                        # Same structure as dev/
│
├── dag.py                             # Your Airflow DAG definition
└── manifest.json                      # Generated by `dbt compile`
```

**Key points:**
- `config/base/*.yml` contains defaults shared across all environments
- `config/{env}/*.yml` contains environment-specific overrides
- Files in `config/{env}/` are merged on top of `config/base/` (environment values take precedence)
- Only include files in `config/{env}/` that override base values

## Rollback Plan

If you encounter critical issues, you can rollback to v0.35.0:

```bash
pip install dbt-airflow-factory==0.35.0
```

**Note:** v0.35.0 will continue to receive critical bug fixes for 6 months after v1.0.0 release.

## Getting Help

- **GitHub Issues:** [github.com/getindata/dbt-airflow-factory/issues](https://github.com/getindata/dbt-airflow-factory/issues)
- **Changelog:** See [CHANGELOG.md](CHANGELOG.md) for detailed changes
- **Cosmos Documentation:** [astronomer-cosmos.readthedocs.io](https://astronomer-cosmos.readthedocs.io)

## Summary

The v1.0.0 migration is designed to be **zero-effort** for most users:

1. ✅ Update `requirements.txt` to `dbt-airflow-factory==1.0.0`
2. ✅ Run `pip install --upgrade`
3. ✅ Test your DAGs
4. ✅ Deploy to production

**No configuration changes. No code changes. Just upgrade and go.**

The internal switch to Cosmos provides a more maintainable, battle-tested foundation while preserving all existing functionality and configurations.
