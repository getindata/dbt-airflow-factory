# Cosmos Integration Capability

## ADDED Requirements

### Requirement: Cosmos-Based dbt Execution
The system SHALL use Astronomer Cosmos library (>=1.11.0, <2.0) as the execution engine for dbt tasks in Airflow DAGs.

#### Scenario: Cosmos DbtTaskGroup creation
- **GIVEN** valid dbt project configuration and Kubernetes execution environment config
- **WHEN** AirflowDagFactory.create() is called
- **THEN** a Cosmos DbtTaskGroup SHALL be created with ProjectConfig, ProfileConfig, ExecutionConfig, and operator_args
- **AND** the DbtTaskGroup SHALL automatically parse the dbt manifest.json
- **AND** each dbt model SHALL become an individual Airflow task

#### Scenario: Model-level task granularity
- **GIVEN** a dbt project with 10 models in manifest.json
- **WHEN** the DAG is generated
- **THEN** 10 individual Airflow tasks SHALL be created (one per dbt model)
- **AND** task dependencies SHALL match dbt model dependencies from the manifest

#### Scenario: Automatic seed handling
- **GIVEN** a dbt project with seed CSV files in the manifest
- **WHEN** the DAG is generated using Cosmos
- **THEN** seed tasks SHALL be automatically created from the manifest
- **AND** no explicit seed_task configuration SHALL be required in airflow.yml

### Requirement: ProjectConfig Translation
The system SHALL translate dbt.yml configuration to Cosmos ProjectConfig object.

#### Scenario: Basic project configuration
- **GIVEN** dbt.yml with project_dir_path="/dbt" and manifest_path="/dbt/manifest.json"
- **WHEN** translate_configs() is called
- **THEN** a ProjectConfig SHALL be created with dbt_project_path="/dbt"
- **AND** manifest_path SHALL be set to "/dbt/manifest.json"

#### Scenario: Project name and path handling
- **GIVEN** dbt.yml with dbt_project_name="my_project" and dbt_project_path="/custom/path"
- **WHEN** translate_configs() is called
- **THEN** ProjectConfig SHALL include the project name and custom path

#### Scenario: ProjectConfig and ExecutionConfig mutual exclusivity
- **GIVEN** manifest_path and project_name are both provided in configuration
- **WHEN** build_project_config() is called
- **THEN** dbt_project_path SHALL NOT be set in ProjectConfig
- **AND** dbt_project_path SHALL be set in ExecutionConfig instead
- **REASON** Cosmos throws CosmosValueError if both ProjectConfig.dbt_project_path and ExecutionConfig.dbt_project_path are set

#### Scenario: Project name extraction from manifest
- **GIVEN** manifest.json with nodes containing "model.my_project.customers"
- **WHEN** extract_project_name_from_manifest() is called
- **THEN** project_name "my_project" SHALL be extracted from the second part of node ID
- **AND** extraction SHALL work for both nodes and sources sections

#### Scenario: DAG ID validation against project name
- **GIVEN** dag_id="my_dag" and manifest contains project_name="dbt_test"
- **AND** no explicit dbt_project_name set in dbt.yml
- **WHEN** build_project_config() is called
- **THEN** ValueError SHALL be raised with migration instruction
- **AND** error message SHALL suggest adding 'dbt_project_name: dbt_test' to config/base/dbt.yml

#### Scenario: DAG ID validation bypassed with explicit project name
- **GIVEN** dag_id="my_dag" and manifest contains project_name="dbt_test"
- **AND** dbt.yml has dbt_project_name="dbt_test" explicitly set
- **WHEN** build_project_config() is called
- **THEN** no validation error SHALL be raised
- **AND** ProjectConfig SHALL use the explicit project name

#### Scenario: Optional manifest_file_name configuration
- **GIVEN** airflow.yml with manifest_file_name="custom/path/manifest.json"
- **WHEN** DAG is generated
- **THEN** manifest SHALL be loaded from the specified path relative to working directory
- **DEFAULT** manifest.json if not specified

#### Scenario: Optional dbt_project_name configuration
- **GIVEN** dbt.yml with dbt_project_name="my_project"
- **WHEN** build_project_config() is called
- **THEN** project_name SHALL be set to "my_project"
- **AND** DAG ID validation SHALL be skipped (explicit name takes precedence)

### Requirement: ProfileConfig Translation
The system SHALL translate dbt.yml profile configuration to Cosmos ProfileConfig object.

#### Scenario: Profile target configuration
- **GIVEN** dbt.yml with target="production" and profile_dir_path="/root/.dbt"
- **WHEN** translate_configs() is called
- **THEN** a ProfileConfig SHALL be created with target_name="production"
- **AND** profiles_yml_filepath SHALL be set to "/root/.dbt"

### Requirement: ExecutionConfig Translation
The system SHALL translate execution_env.yml to Cosmos ExecutionConfig with appropriate ExecutionMode.

#### Scenario: Kubernetes execution mode
- **GIVEN** execution_env.yml with type="k8s" and image config
- **WHEN** translate_configs() is called
- **THEN** ExecutionConfig SHALL be created with ExecutionMode.KUBERNETES
- **AND** image configuration SHALL be set from execution_env.yml

#### Scenario: Local/Bash execution mode
- **GIVEN** execution_env.yml with type="bash"
- **WHEN** translate_configs() is called
- **THEN** ExecutionConfig SHALL be created with ExecutionMode.LOCAL

#### Scenario: Docker execution mode
- **GIVEN** execution_env.yml with type="docker"
- **WHEN** translate_configs() is called
- **THEN** ExecutionConfig SHALL be created with ExecutionMode.DOCKER

### Requirement: Transparent Kubernetes Configuration Pass-Through
The system SHALL pass k8s.yml configuration dictionary directly to Cosmos operator_args without explicit parameter mapping.

#### Scenario: Complete k8s config pass-through
- **GIVEN** k8s.yml with namespace, secrets, tolerations, affinity, volumes, and other K8s parameters
- **WHEN** build_operator_args() is called
- **THEN** the entire k8s.yml config dictionary SHALL be passed to operator_args
- **AND** all KubernetesPodOperator parameters SHALL be preserved unchanged
- **AND** no explicit parameter-by-parameter mapping SHALL be performed

#### Scenario: Kubernetes secrets configuration
- **GIVEN** k8s.yml with secrets defined (deploy_type: env and volume)
- **WHEN** operator_args are built
- **THEN** all secret configurations SHALL be passed through to Cosmos
- **AND** Cosmos SHALL handle secret mounting and environment variable injection

#### Scenario: Kubernetes resource limits and requests
- **GIVEN** k8s.yml with resources.limit and resources.requests (cpu, memory)
- **WHEN** operator_args are built
- **THEN** resource configuration SHALL be passed through unchanged
- **AND** Airflow 2.3+ container_resources format SHALL be supported

#### Scenario: Node selectors and tolerations
- **GIVEN** k8s.yml with node_selectors and tolerations
- **WHEN** operator_args are built
- **THEN** scheduling configuration SHALL be passed through to Cosmos
- **AND** pods SHALL be scheduled according to these constraints

### Requirement: DataHub Environment Variable Injection
The system SHALL merge DataHub configuration from datahub.yml into Kubernetes environment variables.

#### Scenario: DataHub env var injection
- **GIVEN** datahub.yml with DATAHUB_GMS_URL="http://datahub:8080"
- **AND** k8s.yml with existing envs: {APP_ENV: "prod"}
- **WHEN** build_operator_args() is called
- **THEN** operator_args env SHALL include both DATAHUB_GMS_URL and APP_ENV
- **AND** DataHub env vars SHALL be merged with existing K8s envs

#### Scenario: Missing datahub.yml
- **GIVEN** no datahub.yml file exists
- **WHEN** build_operator_args() is called
- **THEN** operator_args SHALL be built successfully without DataHub env vars
- **AND** no error SHALL be raised

### Requirement: Cosmos.yml Advanced Overrides
The system SHALL support optional cosmos.yml file for advanced Cosmos-specific configuration overrides.

#### Scenario: Load mode override
- **GIVEN** cosmos.yml with load_mode="dbt_manifest"
- **WHEN** translate_configs() is called
- **THEN** Cosmos DbtTaskGroup SHALL use the specified load_mode
- **AND** default "automatic" mode SHALL be overridden

#### Scenario: Render config override
- **GIVEN** cosmos.yml with render_config: {select: ["tag:daily"], exclude: ["tag:deprecated"]}
- **WHEN** translate_configs() is called
- **THEN** Cosmos SHALL apply the render_config filters
- **AND** only models matching the selection SHALL be included

#### Scenario: Operator args override
- **GIVEN** cosmos.yml with operator_args: {install_deps: true}
- **AND** k8s.yml with existing operator configuration
- **WHEN** translate_configs() is called
- **THEN** cosmos.yml operator_args SHALL be merged with k8s.yml config
- **AND** cosmos.yml values SHALL take precedence for conflicting keys

#### Scenario: Missing cosmos.yml
- **GIVEN** no cosmos.yml file exists
- **WHEN** translate_configs() is called
- **THEN** translation SHALL succeed with default Cosmos behavior
- **AND** no advanced overrides SHALL be applied

### Requirement: Backward Compatibility with v0.35.0 Configs
The system SHALL maintain 100% backward compatibility with existing v0.35.0 YAML configuration files.

#### Scenario: Existing airflow.yml compatibility
- **GIVEN** airflow.yml from v0.35.0 with DAG config and seed_task settings
- **WHEN** AirflowDagFactory.create() is called
- **THEN** DAG SHALL be created successfully
- **AND** seed_task configuration SHALL be ignored (Cosmos handles seeds)
- **AND** all other airflow.yml settings SHALL be respected

#### Scenario: Existing dbt.yml compatibility
- **GIVEN** dbt.yml from v0.35.0 with target, profile_dir_path, project_dir_path
- **WHEN** translate_configs() is called
- **THEN** all dbt.yml settings SHALL map correctly to Cosmos configs
- **AND** no configuration changes SHALL be required

#### Scenario: Existing k8s.yml compatibility
- **GIVEN** k8s.yml from v0.35.0 with all legacy K8s parameters
- **WHEN** translate_configs() is called
- **THEN** all K8s parameters SHALL pass through to Cosmos
- **AND** no configuration changes SHALL be required

### Requirement: Manifest Parsing Replacement
The system SHALL use Cosmos built-in manifest parsing instead of dbt-graph-builder library.

#### Scenario: dbt-graph-builder removal
- **GIVEN** the dbt-airflow-factory v1.0.0 codebase
- **WHEN** dependencies are inspected
- **THEN** dbt-graph-builder SHALL NOT be present in requirements
- **AND** astronomer-cosmos>=1.11.0 SHALL be the only dbt parsing dependency

#### Scenario: Manifest parsing via Cosmos
- **GIVEN** a dbt manifest.json file
- **WHEN** Cosmos DbtTaskGroup is created
- **THEN** Cosmos SHALL parse the manifest internally
- **AND** no dbt-graph-builder code SHALL be executed

### Requirement: Airflow Version Compatibility
The system SHALL support Airflow versions 2.5 through 2.11 via Cosmos.

#### Scenario: Airflow 2.5 compatibility
- **GIVEN** Apache Airflow 2.5.x environment
- **WHEN** DAG is generated with Cosmos
- **THEN** DAG SHALL parse and execute successfully
- **AND** all Cosmos features SHALL work correctly

#### Scenario: Airflow 2.11 compatibility
- **GIVEN** Apache Airflow 2.11.x environment
- **WHEN** DAG is generated with Cosmos
- **THEN** DAG SHALL parse and execute successfully
- **AND** latest provider package versions SHALL be used

### Requirement: dbt Version Compatibility
The system SHALL support dbt versions 1.7 through 1.10 as peer dependencies via Cosmos.

#### Scenario: dbt 1.7 compatibility
- **GIVEN** dbt-core 1.7.x installed
- **WHEN** Cosmos parses the manifest
- **THEN** manifest SHALL be parsed successfully
- **AND** all dbt 1.7 features SHALL be supported

#### Scenario: dbt 1.10 compatibility
- **GIVEN** dbt-core 1.10.x installed
- **WHEN** Cosmos parses the manifest
- **THEN** manifest SHALL be parsed successfully
- **AND** all dbt 1.10 features SHALL be supported

#### Scenario: No strict dbt version pinning
- **GIVEN** setup.py dependencies
- **WHEN** dependencies are installed
- **THEN** dbt-core SHALL NOT have a strict version constraint
- **AND** Cosmos SHALL manage dbt compatibility as a peer dependency
