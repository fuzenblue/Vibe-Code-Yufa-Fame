"""
NYC Taxi Trips Pipeline DAG
============================
ETL pipeline: Ingest → Clean → Transform → Load to MySQL star-schema

Generated from prompts in: nyc-taxi-trips-pipeline/prompts/
"""
import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add pipeline code directory to Python path
sys.path.insert(0, "/opt/airflow/pipelines/nyc-taxi-trips-pipeline/code")

from ingest_taxi_data import ingest_taxi_data # ingest raw data from API
# from clean_taxi_data import clean_taxi_data
# from transform_taxi_data import transform_taxi_data
# from load_taxi_model import load_taxi_model

default_args = {
    "owner": "vibe-coder",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_trips_pipeline",
    default_args=default_args,
    description="NYC Yellow Taxi Trips ETL: Ingest → Clean → Transform → Load",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc", "taxi", "etl", "vibe-code"],
) as dag:

    t1_ingest = PythonOperator(
        task_id="ingest_taxi_data",
        python_callable=ingest_taxi_data,
    )

    # t2_clean = PythonOperator(
    #     task_id="clean_taxi_data",
    #     python_callable=clean_taxi_data,
    # )

    # t3_transform = PythonOperator(
    #     task_id="transform_taxi_data",
    #     python_callable=transform_taxi_data,
    # )

    # t4_load = PythonOperator(
    #     task_id="load_taxi_model",
    #     python_callable=load_taxi_model,
    # )

    t1_ingest 
    # >> t2_clean >> t3_transform >> t4_load
