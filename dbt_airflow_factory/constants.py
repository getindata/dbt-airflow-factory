import airflow

IS_FIRST_AIRFLOW_VERSION = airflow.__version__.startswith("1.")
IS_AIRFLOW_NEWER_THAN_2_4 = not IS_FIRST_AIRFLOW_VERSION and (
    not airflow.__version__.startswith("2.") or int(airflow.__version__.split(".")[1]) > 4
)
