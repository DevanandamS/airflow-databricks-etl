from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def bronze_task():

    env = Variable.get("environment")
    print(
        f"Submitting Bronze Databricks Job in {env}"
    )

def silver_task():

    env = Variable.get("environment")
    print(
        f"Submitting Silver Databricks Job in {env}"
        )

def gold_task():

    env = Variable.get("environment")
    print(
        f"Submitting Gold Databricks Job in {env}"
        )


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}


with DAG(
    dag_id="online_retail_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "databricks"]
) as dag:

    bronze = PythonOperator(
        task_id="bronze_task",
        python_callable=bronze_task
    )

    silver = PythonOperator(
        task_id="silver_task",
        python_callable=silver_task
    )

    gold = PythonOperator(
        task_id="gold_task",
        python_callable=gold_task
    )

    bronze >> silver >> gold