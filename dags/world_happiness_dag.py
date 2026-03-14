"""
World Happiness Pipeline DAG
==============================
ETL pipeline: Ingest → Transform → Clean → Load to MySQL star-schema

Note: This pipeline has a different step order than usual:
  Ingest → Transform (normalize columns) → Clean (outliers/validation) → Load

Generated from prompts in: world-happiness-pipeline/prompts/
"""
import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add pipeline code directory to Python path
sys.path.insert(0, "/opt/airflow/pipelines/world-happiness-pipeline/code")

from ingest_happiness_data import ingest_happiness_data
from transform_happiness_data import transform_happiness_data
from clean_happiness_data import clean_happiness_data
from load_happiness_model import load_happiness_model

default_args = {
    "owner": "vibe-coder",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="world_happiness_pipeline",
    default_args=default_args,
    description="World Happiness Report ETL: Ingest → Transform → Clean → Load",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["happiness", "world", "etl", "vibe-code"],
) as dag:

    t1_ingest = PythonOperator(
        task_id="ingest_happiness_data",
        python_callable=ingest_happiness_data,
    )

    t2_transform = PythonOperator(
        task_id="transform_happiness_data",
        python_callable=transform_happiness_data,
    )

    t3_clean = PythonOperator(
        task_id="clean_happiness_data",
        python_callable=clean_happiness_data,
    )

    t4_load = PythonOperator(
        task_id="load_happiness_model",
        python_callable=load_happiness_model,
    )

    # Transform first (normalize columns across years), then Clean
    t1_ingest >> t2_transform >> t3_clean >> t4_load
