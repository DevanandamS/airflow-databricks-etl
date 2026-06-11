FROM apache/airflow:2.10.5

RUN pip install dbt-databricks==1.12.0

RUN mkdir -p /opt/airflow/.dbt
RUN mkdir -p /opt/airflow/dbt

ENV DBT_PROFILES_DIR=/opt/airflow/.dbt