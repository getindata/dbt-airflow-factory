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
     -
     - Docker image repository URL
   * - image.tag
     - string
     - x
     -
     - Docker image tag
   * - type
     - string
     - x
     -
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

Example files
+++++++++++++++++++

It is best to look up the example configuration files in
`tests directory <https://github.com/getindata/dbt-airflow-factory/tree/develop/tests/config>`_ to get a glimpse
of correct configs.
