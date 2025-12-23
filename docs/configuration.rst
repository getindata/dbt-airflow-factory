Configuration
-----

Description
+++++++++++++++++++

airflow.yml file
~~~~~~~~~~~~~~~~~~~~~~~
.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - default_args
     - dictionary
     - x
     -
     - Values that are passed to DAG as default_arguments (check Airflow documentation for more details)
   * - dag
     - dictionary
     - x
     -
     - Values used to DAG creation. Currently ``dag_id``, ``description``, ``schedule_interval`` and ``catchup`` are supported. Check Airflow documentation for more details about each of them.
   * - seed_task
     - boolean
     -
     - False
     - Enables first task of the DAG that is responsible for executing *dbt seed* command to load some data.
   * - manifest_file_name
     - string
     -
     - manifest.json
     - Name of the file with DBT manifest.
   * - use_task_group
     - boolean
     -
     - False
     - Enable grouping ``dbt run`` and ``dbt test`` into Airflow's Task Groups. It is only available in Airflow 2+ (check Airflow documentation for more details).
   * - show_ephemeral_models
     - boolean
     -
     - True
     - Enabled/disables separate tasks for DBT's ephemeral models. The tasks are finished in second as they have nothing to do.
   * - failure_handlers
     - list
     -
     - empty list
     - Each item of the list contains configuration of notifications handler in case Tasks or DAG fails. Each item is a dictionary with following fields
       ``type`` (type of handler eg. *slack*, *teams* or *google_chat*), ``webserver_url`` (Airflow Webserver URL), ``connection_id`` (id of the Airflow's connection) and ``message_template`` that will be sent.
       More on how to configure the webhooks can be found here: `Slack <https://airflow.apache.org/docs/apache-airflow-providers-slack/stable/_api/airflow/providers/slack/operators/slack_webhook/index.html>`_, `MS Teams <https://code.mendhak.com/Airflow-MS-Teams-Operator/#copy-hook-and-operator>`_ or `Google Chat <https://developers.google.com/workspace/chat/quickstart/webhooks>`_
   * - enable_project_dependencies
     - boolean
     -
     - False
     - When True it creates sensors for all sources that have dag name in metadata. The sources wait for selected DAGs to finish.
   * - save_points
     - list of string
     -
     - empty list
     - List of schemas between which the gateway should be created.

dbt.yml file
~~~~~~~~~~~~~~~~~~~~~~~
.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - target
     - string
     - x
     -
     - Values that are passed to DAG as default_arguments (check Airflow documentation for more details)
   * - project_dir_path
     - string
     -
     - /dbt
     - Values used to DAG creation. Currently ``dag_id``, ``description``, ``schedule_interval`` and ``catchup`` are supported. Check Airflow documentation for more details about each of them.
   * - profile_dir_path
     - string
     -
     - /root/.dbt
     - Enables first task of the DAG that is responsible for executing ``dbt seed`` command to load some data.
   * - vars
     - dictionary
     -
     -
     - Dictionary of variables passed to DBT tasks.

execution_env.yml file
~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 20 2 53
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Description
   * - image.repository
     - string
     - x
     - Docker image repository URL
   * - image.tag
     - string
     - x
     - Docker image tag
   * - type
     - string
     - x
     - Selects type of execution environment. Currently only k8s is available.

k8s.yml file
~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 20 2 53
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Description
   * - image_pull_policy
     - string
     - x
     - See kubernetes documentation for details: https://kubernetes.io/docs/concepts/containers/images/#image-pull-policy
   * - namespace
     - string
     - x
     - Name of the namespace to run processing
   * - envs
     - dictionary
     -
     - Environment variables that will be passed to container
   * - secrets
     - list of dictionaries
     -
     - List that contains secrets mounted to each container. It is required to set ``secret`` as name, ``deploy_type`` (env or volume) and ``deploy_target`` which is path for volume type and name for envs.
   * - labels
     - dictionary
     -
     - Dictionary that contains labels set on created pods
   * - annotations
     - dictionary
     -
     - Annotations applied to created pods
   * - is_delete_operator_pod
     - boolean
     -
     - If set to True finished containers will be deleted
   * - config_file
     - string
     -
     - Path to the k8s configuration available in Airflow
   * - resources.node_selectors
     - dictionary
     -
     - See more details in Kubernetes documentation: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector
   * - resources.tolerations
     - list of dictionaries
     -
     - See more details in Kubernetes documentation: https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
   * - resources.limit
     - dictionary
     -
     - See more details in Kubernetes documentation: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
   * - resources.requests
     - dictionary
     -
     - See more details in Kubernetes documentation: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
   * - execution_script
     - str
     -
     - Script that will be executed inside pod
   * - in_cluster
     - bool
     -
     - Run kubernetes client with in_cluster configuration
   * - cluster_context
     - str
     -
     - Context that points to kubernetes cluster, ignored when ``in_cluster`` is ``True``. If ``None``, current-context is used.
   * - startup_timeout_seconds
     - int
     -
     - Timeout in seconds to startup the pod.

airbyte.yml file
~~~~~~~~~~~~~~~~~~~~~~~
.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - airbyte_connection_id
     - string
     - x
     -
     - Connection id for Airbyte in Airflow instance. Remember to add this to Airflow's dependencies
       ``apache-airflow-providers-airbyte`` to be able to add such connection.
   * - tasks
     - list of objects
     - x
     -
     - Each task consist of fields

       **task_id**: string - name of the task which will be shown on airflow

       **connection_id**: string - id of Airbyte connection

       **asyncrounous**: boolean - Flag to get job_id after submitting the job to the Airbyte API.

       **api_version**: string - Airbyte API version

       **wait_seconds**: integer - Number of seconds between checks. Only used when ``asynchronous`` is False

       **timeout**: float - The amount of time, in seconds, to wait for the request to complete


ingestion.yml file
~~~~~~~~~~~~~~~~~~~~~~~
.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - enable
     - boolean
     - x
     -
     - Boolean value to specify if ingestion task should be added to Airflow's DAG.
   * - engine
     - string
     - x
     -
     - Enumeration based option, currently only supported value is ``airbyte``


airbyte.yml file
~~~~~~~~~~~~~~~~~~~~~~~
.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - airbyte_connection_id
     - string
     - x
     -
     - Connection id for airbyte in airflow instance. Remember to add this to airflow dependencies
       ``apache-airflow-providers-airbyte`` to be able to add such connection.
   * - tasks
     - list of objects
     - x
     -
     - Each task consist of fields

       **task_id**: string - name of the task which will be shown in Airflow

       **connection_id**: string - id of Airbyte connection

       **asyncrounous**: boolean - Flag to get job_id after submitting the job to the Airbyte API.

       **api_version**: string - Airbyte API version

       **wait_seconds**: integer - Number of seconds between checks. Only used when ``asynchronous`` is ``False``

       **timeout**: float - The amount of time, in seconds, to wait for the request to complete


ingestion.yml file
~~~~~~~~~~~~~~~~~~~~~~~
.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - enable
     - boolean
     - x
     -
     - Boolean value to specify if ingestion task should be added to airflow dag.
   * - engine
     - string
     - x
     -
     - Enumeration based option, currently only supported value is ``airbyte``


airbyte.yml file
~~~~~~~~~~~~~~~~~~~~~~~
.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - airbyte_connection_id
     - string
     - x
     -
     - Connection id for airbyte in airflow instance. Remember to add this to Airflow's dependencies
       ``apache-airflow-providers-airbyte`` to be able to add such connection.
   * - tasks
     - list of objects
     - x
     -
     - Each task consist of fields

       **task_id**: string - name of the task which will be shown on airflow

       **connection_id**: string - id of airbyte connection

       **asyncrounous**: boolean - Flag to get job_id after submitting the job to the Airbyte API.

       **api_version**: string - Airbyte API version

       **wait_seconds**: integer - Number of seconds between checks. Only used when ``asynchronous`` is False

       **timeout**: float - The amount of time, in seconds, to wait for the request to complete


ingestion.yml file
~~~~~~~~~~~~~~~~~~~~~~~
.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - enable
     - boolean
     - x
     -
     - Boolean value to specify if ingestion task should be added to Airflow's DAG.
   * - engine
     - string
     - x
     -
     - Enumeration based option, currently only supported value is ``airbyte``

datahub.yml file
~~~~~~~~~~~~~~~~~~~~~~~

This configuration file enables DataHub integration by injecting environment variables into dbt tasks. Place in ``config/{env}/datahub.yml`` (e.g., ``config/production/datahub.yml``).

.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - datahub_gms_url
     - string
     -
     -
     - DataHub GMS (Graph Metadata Service) server URL. This field is informational and not directly used by the factory.
   * - datahub_env_vars
     - dictionary
     -
     - empty dict
     - Dictionary of environment variables to inject into all dbt tasks for DataHub integration. These variables are automatically merged with other environment variables from ``k8s.yml`` and ``cosmos.yml``. Common variables include ``DATAHUB_GMS_URL``, ``DATAHUB_ENV``, and ``DATAHUB_PLATFORM_INSTANCE``.

**Example datahub.yml:**

.. code-block:: yaml

   # config/production/datahub.yml
   datahub_gms_url: "http://datahub-gms:8080"
   datahub_env_vars:
     DATAHUB_GMS_URL: "http://datahub-gms:8080"
     DATAHUB_ENV: "PROD"
     DATAHUB_PLATFORM_INSTANCE: "production-snowflake"

cosmos.yml file
~~~~~~~~~~~~~~~~~~~~~~~

This configuration file provides advanced Cosmos-specific overrides for fine-tuning dbt task execution. Place in ``config/{env}/cosmos.yml`` (e.g., ``config/production/cosmos.yml``). All fields are optional and override defaults from other configuration files.

For complete documentation on available Cosmos parameters, see the `Astronomer Cosmos documentation <https://astronomer.github.io/astronomer-cosmos/>`_.

.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - load_mode
     - string
     -
     - auto-detect
     - How Cosmos loads the dbt project. Options: ``dbt_manifest`` (use manifest.json), ``dbt_ls`` (run dbt ls), ``custom`` (custom loader). Typically auto-detected, override only if needed.
   * - operator_args
     - dictionary
     -
     - empty dict
     - Dictionary of arguments passed to Cosmos DbtTaskGroup operators. These override k8s.yml and other configurations. See operator_args section below for available parameters.
   * - execution_config
     - dictionary
     -
     - empty dict
     - Dictionary of Cosmos ExecutionConfig parameters. See execution_config section below for available parameters.

**operator_args Section:**

The ``operator_args`` section supports all Cosmos/KubernetesPodOperator parameters. Common parameters:

.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - install_deps
     - boolean
     -
     - False
     - Run ``dbt deps`` before executing dbt commands to install dependencies.
   * - full_refresh
     - boolean
     -
     - False
     - Force full refresh of incremental models (equivalent to ``dbt run --full-refresh``).
   * - dbt_executable_path
     - string
     -
     - dbt
     - Path to the dbt executable. Use this to specify custom dbt wrappers or non-standard installations. Replaces deprecated ``execution_script`` from ``execution_env.yml``.
   * - dbt_cmd_global_flags
     - list of strings
     -
     - empty list
     - Flags applied globally to all dbt commands (e.g., ``["--no-write-json", "--debug"]``).
   * - dbt_cmd_flags
     - list of strings
     -
     - empty list
     - Flags applied to specific dbt commands (e.g., ``["--full-refresh", "--fail-fast"]``).
   * - image_pull_policy
     - string
     -
     -
     - Kubernetes image pull policy. Overrides value from ``k8s.yml``. Options: ``Always``, ``IfNotPresent``, ``Never``.
   * - envs
     - dictionary
     -
     - empty dict
     - Environment variables to inject into dbt tasks. These are merged with variables from ``k8s.yml`` and ``datahub.yml``. Cosmos variables take precedence.
   * - retry_on_failure
     - boolean
     -
     - True
     - Whether to retry failed tasks.
   * - retries
     - integer
     -
     - 0
     - Number of times to retry failed tasks.

**execution_config Section:**

The ``execution_config`` section configures how Cosmos executes dbt commands:

.. list-table::
   :widths: 25 20 2 13 40
   :header-rows: 1

   * - Parameter
     - Data type
     - Required
     - Default
     - Description
   * - invocation_mode
     - string
     -
     - subprocess
     - How dbt commands are invoked. Options: ``subprocess`` (run as subprocess), ``dbt_runner`` (use dbt Python API). Use ``subprocess`` for most cases.
   * - test_indirect_selection
     - string
     -
     - eager
     - How dbt selects tests when models are selected. Options: ``eager`` (all tests), ``cautious`` (only direct tests), ``buildable`` (tests for buildable models). See dbt documentation for details.
   * - dbt_executable_path
     - string
     -
     - dbt
     - Alternative location to specify dbt executable path. Can also be set in ``operator_args``.
   * - virtualenv_dir
     - string
     -
     -
     - Path to Python virtual environment containing dbt. Only used when execution_mode is LOCAL or VIRTUALENV.

**Example cosmos.yml files:**

.. code-block:: yaml

   # config/production/cosmos.yml - Production with conservative settings
   operator_args:
     install_deps: false
     full_refresh: false
     dbt_executable_path: "/usr/local/bin/dbt"
     retry_on_failure: true
     retries: 2
     dbt_cmd_global_flags:
       - "--no-write-json"
     envs:
       DBT_ENV: "production"

.. code-block:: yaml

   # config/development/cosmos.yml - Development with full refresh
   operator_args:
     install_deps: true
     full_refresh: true
     dbt_cmd_flags:
       - "--full-refresh"
       - "--debug"
     envs:
       DBT_ENV: "development"
       DBT_DEBUG: "true"

.. code-block:: yaml

   # config/staging/cosmos.yml - Override k8s image pull policy
   operator_args:
     image_pull_policy: "Always"
     install_deps: true
     envs:
       DBT_ENV: "staging"
   execution_config:
     test_indirect_selection: "cautious"

Example files
+++++++++++++++++++

It is best to look up the example configuration files in
`tests directory <https://github.com/getindata/dbt-airflow-factory/tree/develop/tests/config>`_ to get a glimpse
of correct configs.
