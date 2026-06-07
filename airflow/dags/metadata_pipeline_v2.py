from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator

from datetime import datetime

import requests


def get_pipeline_metadata():

    return {

        "online_retail_bronze": {
            "layer": "bronze",
            "job_id": 729150885683146,
            "depends_on": []
        },

        "online_retail_silver": {
            "layer": "silver",
            "job_id": 163860062692317,
            "depends_on": [
                "online_retail_bronze"
            ]
        },

        "online_retail_gold": {
            "layer": "gold",
            "job_id": 553652760992205,
            "depends_on": [
                "online_retail_silver"
            ]
        }
    }


PIPELINES = get_pipeline_metadata()


def trigger_databricks_job(job_id):

    databricks_host = Variable.get(
        "DATABRICKS_HOST"
    )

    databricks_token = Variable.get(
        "DATABRICKS_TOKEN"
    )

    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "job_id": job_id
    }

    response = requests.post(
        f"{databricks_host}/api/2.1/jobs/run-now",
        headers=headers,
        json=payload
    )

    print(
        f"Status Code: {response.status_code}"
    )

    print(
        f"Response: {response.text}"
    )

    response.raise_for_status()


def execute_pipeline(
        pipeline_name):

    job_id = PIPELINES[
        pipeline_name
    ][
        "job_id"
    ]

    print(
        f"Executing {pipeline_name}"
    )

    trigger_databricks_job(
        job_id
    )


with DAG(
    dag_id="metadata_pipeline_v2",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=[
        "metadata",
        "databricks"
    ]
) as dag:

    tasks = {}

    for pipeline_name in PIPELINES:

        tasks[pipeline_name] = PythonOperator(
            task_id=pipeline_name,
            python_callable=execute_pipeline,
            op_args=[
                pipeline_name
            ]
        )

    for pipeline_name, config in PIPELINES.items():

        for dependency in config[
            "depends_on"
        ]:

            tasks[dependency] >> tasks[pipeline_name]

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="""
    export DBT_PROFILES_DIR=/opt/airflow/.dbt
    export DBT_LOG_PATH=/tmp/dbt_logs
    export DBT_TARGET_PATH=/tmp/dbt_target

    cd /opt/airflow/dbt/online_retail_dbt

    dbt run
    """
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="""
    export DBT_PROFILES_DIR=/opt/airflow/.dbt
    export DBT_LOG_PATH=/tmp/dbt_logs
    export DBT_TARGET_PATH=/tmp/dbt_target

    cd /opt/airflow/dbt/online_retail_dbt

    dbt test
    """
)


tasks["online_retail_gold"] >> dbt_run >> dbt_test