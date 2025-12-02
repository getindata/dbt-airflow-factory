# Custom Task Builders Capability

## REMOVED Requirements

### Requirement: Custom dbt Task Builder Factory
**Reason**: Replaced by Cosmos DbtTaskGroup which provides built-in task generation from dbt manifest.

**Migration**: Users require no changes. The AirflowDagFactory API remains unchanged; internally it now uses Cosmos instead of custom builders.

**Previously**: The system provided builder_factory.py to create custom task builders for Kubernetes, ECS, and Bash execution environments.

### Requirement: dbt-graph-builder Manifest Parsing
**Reason**: Cosmos has built-in manifest parsing capabilities, eliminating the need for the dbt-graph-builder dependency.

**Migration**: Users require no changes. Manifest parsing happens automatically via Cosmos.

**Previously**: The system used dbt-graph-builder library (v0.7.x) to parse dbt manifest.json files and extract model dependencies.

### Requirement: Custom Kubernetes Pod Operator Builder
**Reason**: Cosmos generates KubernetesPodOperator tasks internally based on ExecutionMode.KUBERNETES and operator_args.

**Migration**: All k8s.yml configuration parameters pass through transparently to Cosmos operator_args. No configuration changes required.

**Previously**: The system provided k8s/k8s_operator.py to build custom KubernetesPodOperator instances with dbt commands.

### Requirement: Custom ECS Operator Builder
**Reason**: Cosmos provides AWS ECS execution mode (ExecutionMode.AWS_ECS) if supported, or users should use ExecutionMode.DOCKER or ExecutionMode.KUBERNETES.

**Migration**: Update execution_env.yml type from "ecs" to "docker" or "k8s". ECS-specific parameters may need adjustment.

**Previously**: The system provided ecs/ecs_operator.py to build custom ECS operators for running dbt in AWS Fargate.

### Requirement: Custom Bash Operator Builder
**Reason**: Cosmos provides ExecutionMode.LOCAL for bash/local execution.

**Migration**: Update execution_env.yml type from "bash" to remain "bash" (maps to ExecutionMode.LOCAL). No other changes required.

**Previously**: The system provided bash/ module to build BashOperator instances for local dbt execution.

### Requirement: DbtExecutionEnvironmentParameters Class
**Reason**: Cosmos uses ProjectConfig, ProfileConfig, and ExecutionConfig objects instead of custom parameter classes.

**Migration**: Users require no changes. Configuration translation happens internally via cosmos/config_translator.py.

**Previously**: The system provided dbt_parameters.py defining DbtExecutionEnvironmentParameters for dbt configuration.

### Requirement: KubernetesExecutionParameters Class
**Reason**: K8s configuration now passes directly to Cosmos operator_args as a dictionary.

**Migration**: Users require no changes. All k8s.yml parameters pass through transparently.

**Previously**: The system provided k8s/k8s_parameters.py defining KubernetesExecutionParameters for K8s pod configuration.

### Requirement: EcsExecutionParameters Class
**Reason**: ECS-specific parameter handling replaced by standard Cosmos execution configuration.

**Migration**: Update execution_env.yml type from "ecs" to supported Cosmos execution mode.

**Previously**: The system provided ecs/ecs_parameters.py defining EcsExecutionParameters for ECS task configuration.

### Requirement: Custom Task Builder Interface
**Reason**: Cosmos DbtTaskGroup provides a standardized interface for task generation, eliminating the need for custom builder abstractions.

**Migration**: Users require no changes. Internal architecture simplified.

**Previously**: The system provided operator.py defining DbtRunOperatorBuilder interface implemented by K8s, ECS, and Bash builders.

### Requirement: Tasks Builder with Dependency Graph
**Reason**: Cosmos automatically handles dbt model dependencies and task graph construction from the manifest.

**Migration**: Users require no changes. Task dependencies are preserved and managed by Cosmos.

**Previously**: The system provided tasks_builder/builder.py to construct Airflow task dependency graphs from dbt manifest relationships.

### Requirement: Manual Seed Task Creation
**Reason**: Cosmos automatically creates seed tasks from the manifest without requiring explicit configuration.

**Migration**: Remove seed_task: true/false from airflow.yml if desired (it will be ignored). Seeds are now automatically handled.

**Previously**: The system required explicit seed_task configuration in airflow.yml to control whether seed tasks were created.

### Requirement: Ephemeral Operator Handling
**Reason**: Cosmos handles ephemeral models natively through its rendering configuration.

**Migration**: Users require no changes. show_ephemeral_models setting continues to work via Cosmos RenderConfig.

**Previously**: The system provided operator.py with EphemeralOperator (DummyOperator subclass) to represent ephemeral models.

### Requirement: Custom dbt Command Construction
**Reason**: Cosmos constructs dbt commands internally based on node type and configuration.

**Migration**: Users require no changes. dbt command generation is handled by Cosmos.

**Previously**: The system constructed dbt commands with flags like --target, --vars, --project-dir, --profiles-dir, --select in k8s/k8s_operator.py, ecs/ecs_operator.py, and bash/ operators.
