# DAG Generation Capability

## MODIFIED Requirements

### Requirement: DAG Factory API
The system SHALL provide AirflowDagFactory.create() method that generates Airflow DAGs from configuration files, now using Cosmos DbtTaskGroup for dbt task generation.

#### Scenario: DAG creation with Cosmos backend
- **GIVEN** valid configuration files (airflow.yml, dbt.yml, execution_env.yml, k8s.yml)
- **WHEN** AirflowDagFactory(dag_dir, env).create() is called
- **THEN** an Airflow DAG object SHALL be returned
- **AND** dbt tasks SHALL be generated using Cosmos DbtTaskGroup (not custom builders)
- **AND** Airbyte ingestion tasks SHALL be created if configured
- **AND** failure notification callbacks SHALL be attached
- **AND** task dependencies SHALL be wired correctly

#### Scenario: Zero code changes for users
- **GIVEN** user code from v0.35.0: `from dbt_airflow_factory.airflow_dag_factory import AirflowDagFactory; dag = AirflowDagFactory(dag_dir, env).create()`
- **WHEN** upgraded to v1.0.0
- **THEN** the same code SHALL work unchanged
- **AND** DAG SHALL be generated successfully using Cosmos internally

### Requirement: Configuration File Reading
The system SHALL read YAML configuration files from base and environment-specific directories, with no changes to configuration format.

#### Scenario: Config reading unchanged
- **GIVEN** config directory structure: config/base/*.yml and config/{env}/*.yml
- **WHEN** configurations are loaded
- **THEN** config_utils.py SHALL read files using existing logic (unchanged)
- **AND** environment-specific configs SHALL override base configs
- **AND** Airflow template variables SHALL be supported in airflow.yml

### Requirement: Airbyte Integration Preservation
The system SHALL preserve existing Airbyte ingestion task creation, with no changes to ingestion workflow.

#### Scenario: Airbyte ingestion tasks created
- **GIVEN** airbyte.yml or ingestion.yml configuration file
- **WHEN** DAG is generated
- **THEN** AirbyteTriggerSyncOperator tasks SHALL be created before dbt tasks
- **AND** ingestion tasks SHALL depend on start operator
- **AND** dbt tasks SHALL depend on ingestion tasks (ingestion >> dbt_taskgroup)
- **AND** ingestion.py module SHALL remain unchanged

#### Scenario: No ingestion configuration
- **GIVEN** no airbyte.yml or ingestion.yml file
- **WHEN** DAG is generated
- **THEN** no ingestion tasks SHALL be created
- **AND** dbt tasks SHALL depend directly on start operator

### Requirement: Notification Handlers Preservation
The system SHALL preserve existing failure notification handlers (Slack, MS Teams) with no changes to notification workflow.

#### Scenario: Slack failure notifications
- **GIVEN** airflow.yml with failure_handlers: slack webhook configuration
- **WHEN** DAG is generated
- **THEN** on_failure_callback SHALL be attached to all tasks
- **AND** Slack notifications SHALL be sent on task failure
- **AND** notifications/handler.py module SHALL remain unchanged

#### Scenario: MS Teams failure notifications
- **GIVEN** airflow.yml with failure_handlers: MS Teams webhook configuration
- **WHEN** DAG is generated
- **THEN** on_failure_callback SHALL be attached to all tasks
- **AND** MS Teams notifications SHALL be sent on task failure
- **AND** notifications/ms_teams_webhook_operator.py SHALL remain unchanged

### Requirement: DAG Structure Preservation
The system SHALL maintain the same DAG structure with start/end dummy operators and DbtTaskGroup for dbt tasks.

#### Scenario: DAG boundary operators
- **GIVEN** any DAG configuration
- **WHEN** DAG is generated
- **THEN** a start dummy operator SHALL be created
- **AND** an end dummy operator SHALL be created
- **AND** all task chains SHALL connect: start >> [ingestion >>] dbt_taskgroup >> end

#### Scenario: DbtTaskGroup always used
- **GIVEN** any DAG configuration
- **WHEN** DAG is generated using Cosmos
- **THEN** dbt tasks SHALL be grouped in a Cosmos DbtTaskGroup
- **AND** Airflow UI SHALL display tasks hierarchically
- **AND** TaskGroup structure SHALL be used regardless of use_task_group setting

### Requirement: use_task_group Configuration Behavior Change
The system SHALL always use Cosmos DbtTaskGroup for dbt tasks, ignoring the use_task_group configuration setting.

#### Scenario: use_task_group true (no change)
- **GIVEN** airflow.yml with use_task_group: true
- **WHEN** DAG is generated
- **THEN** dbt tasks SHALL be grouped in DbtTaskGroup
- **AND** behavior SHALL be unchanged from v0.35.0

#### Scenario: use_task_group false (behavior change)
- **GIVEN** airflow.yml with use_task_group: false
- **WHEN** DAG is generated with Cosmos
- **THEN** dbt tasks SHALL still be grouped in DbtTaskGroup (Cosmos limitation)
- **AND** use_task_group setting SHALL be ignored
- **AND** no error SHALL be raised
- **AND** tasks SHALL appear grouped in Airflow UI (cosmetic change only)

#### Scenario: use_task_group not specified
- **GIVEN** airflow.yml without use_task_group setting
- **WHEN** DAG is generated
- **THEN** dbt tasks SHALL be grouped in DbtTaskGroup (default behavior)

### Requirement: Seed Task Handling Change
The system SHALL delegate seed task creation to Cosmos automatic manifest parsing, ignoring airflow.yml seed_task configuration.

#### Scenario: seed_task config ignored
- **GIVEN** airflow.yml with seed_task: true
- **WHEN** DAG is generated with Cosmos
- **THEN** seed_task setting SHALL be ignored (no-op)
- **AND** seeds SHALL be created automatically by Cosmos from manifest
- **AND** no error SHALL be raised

#### Scenario: Seeds from manifest
- **GIVEN** dbt manifest.json with seed nodes
- **WHEN** Cosmos DbtTaskGroup is created
- **THEN** seed tasks SHALL be automatically generated
- **AND** seed tasks SHALL run before model tasks per dbt dependencies

### Requirement: Performance Requirements
The system SHALL parse and generate DAGs efficiently, meeting performance targets for various project sizes.

#### Scenario: 100 model DAG parsing
- **GIVEN** a dbt project with 100 models
- **WHEN** DAG is parsed by Airflow scheduler
- **THEN** parsing SHALL complete in less than 5 seconds
- **AND** all 100 model tasks SHALL be created correctly

#### Scenario: 500 model DAG parsing
- **GIVEN** a dbt project with 500 models
- **WHEN** DAG is parsed by Airflow scheduler
- **THEN** parsing SHALL complete in less than 15 seconds
- **AND** all 500 model tasks SHALL be created correctly

### Requirement: Ephemeral Model Handling
The system SHALL respect show_ephemeral_models configuration via Cosmos rendering options.

#### Scenario: Ephemeral models hidden
- **GIVEN** airflow.yml with show_ephemeral_models: false
- **WHEN** DAG is generated
- **THEN** ephemeral dbt models SHALL NOT appear as Airflow tasks
- **AND** ephemeral model logic SHALL be inlined in dependent models

#### Scenario: Ephemeral models shown
- **GIVEN** airflow.yml with show_ephemeral_models: true
- **WHEN** DAG is generated
- **THEN** ephemeral dbt models SHALL appear as Airflow tasks
- **AND** task dependencies SHALL match dbt manifest dependencies
