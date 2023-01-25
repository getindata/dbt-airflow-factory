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
     - Values used to DAG creation. Curerntly dag_id, description, schedule_interval and catchup are supported. Check Airflow documentation for more details about each of them.
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
     - Enable grouping *dbt run* and *dbt test* into Airflow's Task Groups. It is only available in Airflow 2+ (check Airflow documentation for more details).
   * - show_ephemeral_models
     - boolean
     -
     - True
     - Enabled/disables separate tasks for DBT's ephemeral models. The tasks are finisheds in second as they have nothing to do.
   * - failure_handlers
     - list
     -
     -
     - Each item of the list contains configuration of notifications handler in case Tasks / DAG fails. Each item is a dictionary with following fields
        type (type of handler eg. *slack*), connection_id (id of the Airflow's connection) and message_template that will be sent.
   * - enable_project_dependencies
     - boolean
     -
     - False
     - When True it creates sensors for all sources that have dag name in metadata. The sources wait for selected Dags to finish.
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
     - Values used to DAG creation. Curerntly dag_id, description, schedule_interval and catchup are supported. Check Airflow documentation for more details about each of them.
   * - profile_dir_path
     - string
     -
     - /root/.dbt
     - Enables first task of the DAG that is responsible for executing *dbt seed* command to load some data.
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
     - List that contains secrets mounted to each container. It is required to set `secret` as name, `deploy_type` (env or volume) and `deploy_target` which is path for volume type and name for envs.
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
     - Context that points to kubernetes cluster, ignored when in_cluster is True. If None, current-context is used.
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
     - Connection id for airbyte in airflow instance. Remember to add this to airflow dependencies
       `apache-airflow-providers-airbyte` to be able to add such connection.
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
     - Boolean value to specify if ingestion task should be added to airflow dag.
   * - engine
     - string
     - x
     -
     - Enumeration based option, currently only supported value is airbyte


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
       `apache-airflow-providers-airbyte` to be able to add such connection.
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
     - Boolean value to specify if ingestion task should be added to airflow dag.
   * - engine
     - string
     - x
     -
     - Enumeration based option, currently only supported value is airbyte


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
       `apache-airflow-providers-airbyte` to be able to add such connection.
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
     - Boolean value to specify if ingestion task should be added to airflow dag.
   * - engine
     - string
     - x
     -
     - Enumeration based option, currently only supported value is airbyte

Example files
+++++++++++++++++++

It is best to look up the example configuration files in
`tests directory <https://github.com/getindata/dbt-airflow-factory/tree/develop/tests/config>`_ to get a glimpse
of correct configs.
