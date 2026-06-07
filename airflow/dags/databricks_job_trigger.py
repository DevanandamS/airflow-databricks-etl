from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import requests
import os


DATABRICKS_HOST = "https://dbc-b014cc18-afd0.cloud.databricks.com"

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

JOB_ID = 729150885683146


def trigger_databricks_job():

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "job_id": JOB_ID
    }

    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
        headers=headers,
        json=payload
    )

    print("STATUS:", response.status_code)
    print("BODY:", response.text)

    response.raise_for_status()


with DAG(
    dag_id="databricks_job_trigger",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    trigger_job = PythonOperator(
        task_id="trigger_bronze_job",
        python_callable=trigger_databricks_job
    )