from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import requests
import os



DATABRICKS_HOST = "https://dbc-b014cc18-afd0.cloud.databricks.com"

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def test_databricks():

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }

    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.0/workspace/list",
        headers=headers,
        params={"path": "/"}
    )

    print("STATUS:", response.status_code)
    print("BODY:", response.text)

    response.raise_for_status()

with DAG(
    dag_id="databricks_api_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id="test_databricks_connection",
        python_callable=test_databricks
    )