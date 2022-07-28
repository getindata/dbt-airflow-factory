FROM apache/airflow:2.2.5

RUN mkdir -p /opt/airflow/data

COPY requirements.txt /opt/airflow/data
COPY dbt_airflow_factory-0.27.0-py3-none-any.whl /opt/airflow/data

RUN (cd /opt/airflow/data;pip install -r requirements.txt)
RUN (cd /opt/airflow/data;pip install dbt_airflow_factory-0.27.0-py3-none-any.whl)
