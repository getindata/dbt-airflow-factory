# Change: Add Astronomer Cosmos Integration to dbt-airflow-factory

## Why

dbt-airflow-factory has been unmaintained for 2+ years, blocking users from upgrading to Airflow 2.7+ and dbt 1.8+. Astronomer Cosmos (21M+ downloads/month, actively maintained) provides built-in manifest parsing, model-level task granularity, and broad version compatibility (Airflow 2.5-2.11, dbt 1.7-1.10). Migrating to Cosmos eliminates ~1000 lines of custom task builder code while maintaining 100% backward compatibility with existing YAML configurations.

## What Changes

- **BREAKING (internal only):** Replace dbt-graph-builder dependency with astronomer-cosmos (>=1.10.0)
- **BREAKING (internal only):** Remove custom task builders (~1000 lines): tasks_builder/, k8s/, ecs/, bash/, operator.py, dbt_parameters.py, builder_factory.py
- Add new cosmos/ module (~300-400 lines) for config translation: ProjectConfig, ProfileConfig, ExecutionConfig, operator_args builders
- Rewrite AirflowDagFactory.create() to use Cosmos DbtTaskGroup instead of custom builders
- use_task_group config behavior change: always uses DbtTaskGroup (ignores use_task_group: false due to Cosmos limitation)
- seed_task config ignored: Cosmos automatically creates seed tasks from manifest
- Preserve all existing features: Airbyte ingestion, Slack/Teams notifications, DataHub integration, failure callbacks
- Support Airflow 2.5-2.11 and dbt 1.7-1.10 (peer dependency, no strict pins)
- Optional cosmos.yml for advanced Cosmos features (load_mode, render_config overrides)

## Impact

**Affected capabilities:**
- cosmos-integration (ADDED): New Cosmos-based dbt task generation
- dag-generation (MODIFIED): Now uses DbtTaskGroup, use_task_group always true
- custom-task-builders (REMOVED): All custom builder code deleted

**Affected code:**
- airflow_dag_factory.py: Simplified to use Cosmos DbtTaskGroup
- New cosmos/ module: config_translator.py, project_config_builder.py, profile_config_builder.py, execution_config_builder.py, operator_args_builder.py
- Deleted: tasks_builder/, k8s/, ecs/, bash/, operator.py, dbt_parameters.py, builder_factory.py

**User impact:**
- Zero config changes required (100% backward compatible YAML files)
- Zero code changes required (same API: AirflowDagFactory.create())
- Dependency update only: pip install --upgrade dbt-airflow-factory==1.0.0
- Cosmetic change: use_task_group: false no longer respected (tasks always grouped)
- seed_task config ignored (seeds created automatically from manifest)

---

## Detailed Design

**Pure Cosmos approach (v1.0.0):** Complete rewrite using Cosmos as the dbt execution engine, with **100% backward-compatible YAML configs**.

### Strategy: Replace dbt-graph-builder with Cosmos, Preserve All Orchestration Features

**User Experience - ZERO code changes:**

```python
# User code - UNCHANGED
from dbt_airflow_factory.airflow_dag_factory import AirflowDagFactory
dag = AirflowDagFactory(dag_dir, env).create()
```

**Config files - ALL existing configs work as-is:**

```
config/
├── base/
│   ├── airflow.yml       # ✅ UNCHANGED - DAG config, notifications
│   ├── dbt.yml           # ✅ UNCHANGED - dbt project/profile settings
│   ├── execution_env.yml # ✅ UNCHANGED - image + execution type
│   ├── k8s.yml           # ✅ UNCHANGED - K8s pod configuration
│   ├── airbyte.yml       # ✅ UNCHANGED - ingestion tasks
│   └── datahub.yml       # ✅ UNCHANGED - DataHub env vars
└── dev/
    └── dbt.yml           # ✅ UNCHANGED - env overrides
```

**Optional new file for Cosmos-specific overrides:**

```yaml
# cosmos.yml (OPTIONAL - advanced Cosmos features)
load_mode: dbt_manifest  # Override default 'automatic'
render_config:
  select: ["tag:daily"]  # Cosmos-specific selection
  exclude: ["tag:deprecated"]
operator_args:
  install_deps: true     # Install dbt packages automatically
```

### What's Preserved (Non-dbt Orchestration)

All features **outside dbt execution** remain unchanged:

1. **Airbyte Ingestion** (`ingestion.py`)
   - Creates `AirbyteTriggerSyncOperator` tasks before dbt
   - Config: `airbyte.yml` / `ingestion.yml`
   - Dependency: `ingestion_tasks >> dbt_tasks`

2. **Failure Notifications** (`notifications/`)
   - Slack/MS Teams webhooks via `on_failure_callback`
   - Config: `airflow.yml` → `failure_handlers`
   - Applies to all tasks in DAG

3. **DataHub Integration**
   - Environment variables injected into dbt pod/container
   - Config: `datahub.yml` → `DATAHUB_GMS_URL` env var
   - Translated to `operator_args.env` in Cosmos

4. **DAG Structure**
   - Start/end dummy operators for boundaries
   - Task wiring: `ingestion >> dbt_taskgroup >> end`
   - Config reading and validation

### What Changes (dbt Execution Only)

Only dbt task generation is replaced:

1. **Manifest Parsing**: `dbt-graph-builder` → Cosmos parsing modes
2. **Task Creation**: Custom operators → Cosmos `DbtTaskGroup`
3. **Seed Handling**: Cosmos reads seeds from manifest automatically
4. **Execution**: Custom K8s/ECS/Bash builders → Cosmos `ExecutionConfig`

**Code removed (~1000+ lines):**
- `tasks_builder/` - dbt task builder
- `builder_factory.py` - operator factory
- `operator.py` - custom dbt operators
- `k8s/`, `ecs/`, `bash/` - execution operator builders
- `dbt_parameters.py` - dbt config classes

**Code added (~300-400 lines):**
- `cosmos/config_translator.py` - YAML → Cosmos configs
- `cosmos/project_config_builder.py`
- `cosmos/profile_config_builder.py`
- `cosmos/execution_config_builder.py`
- `cosmos/operator_args_builder.py`

### Configuration Mapping

**How existing configs translate to Cosmos:**

```yaml
# execution_env.yml → ExecutionConfig
image:
  repository: gcr.io/my-project/dbt
  tag: "1.7.0"
type: k8s  # Maps to ExecutionMode.KUBERNETES

# k8s.yml → KubernetesPodOperator arguments
namespace: apache-airflow
image_pull_policy: IfNotPresent
envs: {...}              # → operator_args.env
resources: {...}         # → operator_args.requests/limits
labels: {...}            # → operator_args.labels
annotations: {...}       # → operator_args.annotations

# dbt.yml → ProjectConfig + ProfileConfig
target: local
target_type: bigquery
project_dir_path: /dbt   # → ProjectConfig.dbt_project_path
profile_dir_path: /root/.dbt  # → ProfileConfig
```

**What changes internally:**
1. **Remove:** dbt-graph-builder dependency (Cosmos replaces it)
2. **Remove:** Legacy task builder code (~1000+ lines)
3. **Add:** Config translators: YAML → Cosmos Config objects
4. **Simplify:** All dbt logic delegates to Cosmos DbtTaskGroup

**Migration path for users:**
- **v0.35.0 → v1.0.0:** No config changes required!
- **Dependency update:** `pip install --upgrade dbt-airflow-factory==1.0.0`
- **Power users:** Optional cosmos.yml for advanced Cosmos features

### Code Architecture (v1.0.0)

**Clean structure - Cosmos-only:**

```
dbt_airflow_factory/
├── airflow_dag_factory.py       # Main API - simplified
├── config_utils.py               # Config reading - UNCHANGED
├── cosmos/                       # NEW: Cosmos integration
│   ├── __init__.py
│   ├── config_translator.py     # YAML → Cosmos Config objects
│   ├── project_config_builder.py
│   ├── profile_config_builder.py
│   ├── execution_config_builder.py
│   └── operator_args_builder.py
├── ingestion.py                  # Airbyte tasks - UNCHANGED
├── notifications/                # Notifications - UNCHANGED
│   ├── handler.py
│   └── ms_teams_webhook_operator.py
└── constants.py                  # Constants - UNCHANGED

# REMOVED (replaced by Cosmos):
# ├── tasks_builder/              # DELETE
# ├── builder_factory.py          # DELETE
# ├── operator.py                 # DELETE
# ├── k8s/                        # DELETE
# ├── ecs/                        # DELETE
# ├── bash/                       # DELETE
# └── dbt_parameters.py           # DELETE
```

**AirflowDagFactory logic (simplified):**

```python
from cosmos import DbtTaskGroup
from dbt_airflow_factory.cosmos.config_translator import translate_configs

class AirflowDagFactory:
    def create(self) -> DAG:
        # 1. Read existing configs (backward compatible)
        airflow_config = read_config(self.dag_path, self.env, "airflow.yml")
        dbt_config = read_config(self.dag_path, self.env, "dbt.yml")
        exec_env_config = read_config(self.dag_path, self.env, "execution_env.yml")
        k8s_config = read_config(self.dag_path, self.env, "k8s.yml")
        datahub_config = read_config(self.dag_path, self.env, "datahub.yml", optional=True)
        cosmos_config = read_config(self.dag_path, self.env, "cosmos.yml", optional=True)

        # 2. Translate to Cosmos config objects
        project_cfg, profile_cfg, execution_cfg, operator_args = translate_configs(
            dbt_config=dbt_config,
            execution_env_config=exec_env_config,
            k8s_config=k8s_config,
            datahub_config=datahub_config,
            cosmos_config=cosmos_config  # Optional overrides
        )

        # 3. Create DAG with Airflow config
        with DAG(**airflow_config["dag"], default_args=airflow_config["default_args"]) as dag:

            # 4. Airbyte ingestion tasks (if enabled) - UNCHANGED
            ingestion_tasks = self._create_airbyte_tasks()

            # 5. dbt tasks via Cosmos DbtTaskGroup - NEW
            dbt_tg = DbtTaskGroup(
                group_id="dbt",
                project_config=project_cfg,
                profile_config=profile_cfg,
                execution_config=execution_cfg,
                operator_args=operator_args,
            )
            # Cosmos automatically handles seeds from manifest

            # 6. End dummy operator
            end = DummyOperator(task_id="end")

            # 7. Wire dependencies - UNCHANGED PATTERN
            if ingestion_tasks:
                ingestion_tasks >> dbt_tg
            dbt_tg >> end

        return dag
```

## Impact Analysis

### Users

- **Zero config changes:** All existing YAML files work unchanged
- **Zero code changes:** Same API, same import, same DAG creation pattern
- **Dependency update only:** `pip install --upgrade dbt-airflow-factory==1.0.0`
- **Benefits:**
  - dbt 1.7-1.10 support (including 1.8, 1.9, 1.10 features)
  - Airflow 2.5-2.11 support (modern Airflow versions)
  - Model-level task visibility in Airflow UI
  - Better maintained (Cosmos actively developed)
  - More execution modes (docker, kubernetes, local, virtualenv)

### Library Maintainers

- **Maintenance renewed:** Active development resumes on modern foundation
- **Code reduction:** ~700-1000 lines removed, ~300-400 added (net -400-700 lines)
- **Simpler architecture:** Delegate dbt execution to Cosmos, focus on orchestration
- **Modern dependencies:** No more dbt-graph-builder, use actively-maintained Cosmos
- **Clear responsibilities:** dbt-airflow-factory = orchestration, Cosmos = dbt execution

### Ecosystem (data-pipelines-cli, etc.)

- **No changes required:** dp cli continues deploying DAG files unchanged
- **Optional enhancements:** Can add cosmos.yml to templates for advanced features
- **Documentation updates:** Update examples to show v1.0.0 (configs remain same)
- **Backward compatibility:** Users can stay on v0.35.0 if needed (no forced upgrade)

## Dependencies

### Dependency Changes (setup.py)

**Current (v0.35.0):**
```python
INSTALL_REQUIRES = [
    "pytimeparse>=1.1, <2",
    "dbt-graph-builder>=0.7.0, <0.8.0",  # ❌ REMOVE
    "apache-airflow[kubernetes,slack]>=2.5, <3",
    "apache-airflow-providers-airbyte>=3.1, <4",  # ⚠️ UPDATE
]
```

**Proposed (v1.0.0):**
```python
INSTALL_REQUIRES = [
    "pytimeparse>=1.1, <2",
    "apache-airflow>=2.5, <3",  # Keep broad Airflow 2.x support
    "astronomer-cosmos>=1.10.0, <2.0",  # ✅ NEW - core dependency

    # Provider packages - no upper bounds (pip auto-resolves based on Airflow version)
    "apache-airflow-providers-cncf-kubernetes",  # ✅ UPDATED - was [kubernetes] extra
    "apache-airflow-providers-slack",  # ✅ UPDATED - was [slack] extra
    "apache-airflow-providers-airbyte>=3.1",  # ✅ UPDATED - remove <4 upper bound
]
```

**Rationale for removing upper bounds on providers:**
- Provider packages version independently from Airflow core
- Airflow 2.5-2.9 automatically gets compatible older provider versions
- Airflow 2.10-2.11 automatically gets latest provider versions (5.x airbyte, 10.x k8s)
- Follows [Apache Airflow recommendations](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#provider-packages)
- Simplifies dependency management - pip resolves correct versions

### Version Compatibility Matrix

| Component | v0.35.0 | v1.0.0 | Notes |
|-----------|---------|---------|-------|
| **Python** | 3.8-3.11 | 3.8-3.11 | Unchanged |
| **Airflow** | 2.x | 2.5-2.11 | Explicit lower bound |
| **dbt** | Implicit 1.0-1.7 | 1.7-1.10 peer dependency | No strict pin, broader |
| **Airbyte Provider** | 3.1-3.x | 3.1+ (auto-resolved) | Airflow 2.10+ gets v5.x |
| **K8s Provider** | Via extras | Auto-resolved | Airflow 2.10+ gets v10.x |
| **Cosmos** | N/A | 1.11-1.x | New |
| **dbt-graph-builder** | 0.7.x | N/A | Removed |

**Blocks:** None

**Blocked by:** None

**Related projects:** data-pipelines-cli (documentation updates only, no code changes)

## Implementation Timeline

### Development (4-6 weeks)

1. **Week 1-2: Setup & Code Removal**
   - Update dependencies (add Cosmos, remove dbt-graph-builder)
   - Delete legacy code (~1000 lines: tasks_builder/, k8s/, ecs/, bash/, operator.py)
   - Set up project structure for cosmos/ module

2. **Week 3-4: Cosmos Integration**
   - Implement config translators (YAML → Cosmos configs)
   - Rewrite AirflowDagFactory.create() to use DbtTaskGroup
   - Implement DataHub env var injection
   - Preserve Airbyte/notifications integrations

3. **Week 5: Testing**
   - Unit tests for config translators (>90% coverage)
   - Integration tests with real DAG configs
   - Backward compatibility tests (v0.35.0 configs)
   - Airflow 2.5-2.11 + dbt 1.7-1.10 compatibility matrix

4. **Week 6: Documentation & Release**
   - Update README, CHANGELOG, docs
   - Write MIGRATION.md guide
   - Build & test package distribution
   - Publish v1.0.0 to PyPI

## Alternatives Considered

1. **Fork to new library (dp-cosmos-factory)**
   - **Rejected:** Fragments ecosystem, duplicate maintenance
   - **Better:** Evolve existing library with backwards compatibility

2. **Pure Cosmos (no wrapper)**
   - **Rejected:** Loses config-driven workflow, forces users to rewrite DAGs
   - **Better:** Maintain high-level API users expect

3. **Rewrite dbt-airflow-factory from scratch**
   - **Rejected:** Breaks all existing users, high migration cost
   - **Better:** Phased migration with backwards compatibility

4. **Wait for original maintainers**
   - **Rejected:** No activity for 2+ years, blocking real users
   - **Action needed:** Resume maintenance with Cosmos integration

## Key Decisions

All decisions finalized for v1.0.0:

1. **Versioning: v1.0.0 (MAJOR)**
   - Dependency changes = major version bump (semantic versioning)
   - Configs and API unchanged = no breaking changes for users
   - First stable release of modernized library

2. **Seed handling: Cosmos from manifest**
   - No custom seed_task creation logic
   - `airflow.yml` → `seed_task: true/false` becomes no-op (not breaking)
   - Cleaner: Cosmos reads seeds directly from manifest.json

3. **cosmos.yml: Optional**
   - NOT required for basic usage
   - Only for advanced Cosmos features (load_mode, render_config overrides)
   - Power user feature, not migration requirement

4. **Provider packages: No upper bounds**
   - Remove version caps (e.g., `<4`) on providers
   - Let pip auto-resolve based on Airflow version
   - Follows Apache Airflow best practices
   - Airflow 2.5-2.9 gets older providers, 2.10+ gets latest

5. **DataHub: Environment variable injection**
   - `datahub.yml` → `DATAHUB_GMS_URL` env var
   - Injected via `operator_args.env` to Cosmos
   - Same mechanism as v0.35.0, different plumbing

## Success Criteria

**Must have (blocking v1.0.0 release):**

- [ ] All v0.35.0 YAML configs work unchanged
- [ ] Zero code changes required for DAG creation
- [ ] Airbyte ingestion works unchanged
- [ ] Failure notifications (Slack/Teams) work unchanged
- [ ] DataHub env var injection works
- [ ] Seeds created from manifest automatically
- [ ] Airflow 2.5, 2.7, 2.9, 2.11 tested
- [ ] dbt 1.7, 1.8, 1.9, 1.10 tested (peer dependency)
- [ ] Unit test coverage >80%
- [ ] Integration tests for K8s, Bash execution modes
- [ ] Integration test for Airbyte + dbt + notifications
- [ ] Performance: DAG parse <5s for 100 models
- [ ] Code reduction: ~400-700 net lines removed
- [ ] Apache 2.0 license maintained

**Nice to have (post-v1.0.0):**

- [ ] MIGRATION.md guide with examples
- [ ] cosmos.yml advanced features documented
- [ ] Community beta testing (3+ users)
- [ ] Performance benchmarks vs v0.35.0
- [ ] Example DAG repository

## Risk Mitigation

**Risk:** Config translation bugs break existing DAGs

- **Mitigation:** Extensive backward compatibility tests with real v0.35.0 configs from production

**Risk:** Cosmos API changes break integration

- **Mitigation:** Pin Cosmos major version (`>=1.10.0,<2.0`), test new releases before updating

**Risk:** Performance regression

- **Mitigation:** Benchmark DAG parse time target (<5s for 100 models), performance tests in CI

**Risk:** Provider version conflicts

- **Mitigation:** Test matrix across Airflow 2.5-2.11, let pip auto-resolve provider versions

**Risk:** Airbyte/notifications/DataHub integrations break

- **Mitigation:** These modules untouched, extensive integration tests verify preserved functionality

**Risk:** Users confused by seed_task no-op

- **Mitigation:** Document clearly in MIGRATION.md that Cosmos handles seeds automatically from manifest

## License Compliance

**All components use Apache 2.0 license:**
- dbt-airflow-factory: Apache 2.0
- Astronomer Cosmos: Apache 2.0
- Integration: Apache 2.0 (no change)

**No license concerns:** Integration maintains same license, adds compatible dependency.

## Next Steps

1. **Maintainer decision:** Approve resuming active maintenance with Cosmos integration
2. **Repository setup:** Prepare dev environment, create feature branch
3. **Development:** Begin Phase 1 (cosmos_backend implementation)
4. **Community engagement:** Announce plans, invite contributors
5. **Alpha release:** Target alpha release in 4-6 weeks
