from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# ------------------------------------------------------------------
# Metadata (Eventually comes from Databricks metadata tables)
# ------------------------------------------------------------------

PIPELINES = {

    "online_retail_bronze": {
        "layer": "bronze",
        "depends_on": []
    },

    "online_retail_silver": {
        "layer": "silver",
        "depends_on": [
            "online_retail_bronze"
        ]
    },

    "online_retail_gold": {
        "layer": "gold",
        "depends_on": [
            "online_retail_silver"
        ]
    }
}


# ------------------------------------------------------------------
# Generic Executor
# ------------------------------------------------------------------

def execute_pipeline(
        pipeline_name,
        layer):

    print("=" * 50)

    print(
        f"Pipeline: {pipeline_name}"
    )

    print(
        f"Layer: {layer}"
    )

    print(
        f"Passing parameter:"
    )

    print(
        f"pipeline_name={pipeline_name}"
    )

    print(
        "Notebook would execute:"
    )

    print(
        "dbutils.widgets.get('pipeline_name')"
    )

    print("=" * 50)


# ------------------------------------------------------------------
# Default DAG Settings
# ------------------------------------------------------------------

default_args = {

    "owner": "data_engineering",

    "depends_on_past": False,

    "retries": 3,

    "retry_delay": timedelta(minutes=1)
}


# ------------------------------------------------------------------
# DAG Definition
# ------------------------------------------------------------------

with DAG(

    dag_id="metadata_pipeline",

    description="Metadata Driven Pipeline Framework",

    start_date=datetime(2025, 1, 1),

    schedule=None,

    catchup=False,

    default_args=default_args,

    tags=[
        "metadata",
        "framework",
        "airflow"
    ]

) as dag:

    tasks = {}

    # --------------------------------------------------------------
    # Create Tasks
    # --------------------------------------------------------------

    for pipeline_name, pipeline_info in PIPELINES.items():

        tasks[pipeline_name] = PythonOperator(

            task_id=pipeline_name,

            python_callable=execute_pipeline,

            op_args=[
                pipeline_name,
                pipeline_info["layer"]
            ]

        )

    # --------------------------------------------------------------
    # Create Dependencies
    # --------------------------------------------------------------

    for pipeline_name, pipeline_info in PIPELINES.items():

        for dependency in pipeline_info["depends_on"]:

            tasks[dependency] >> tasks[pipeline_name]