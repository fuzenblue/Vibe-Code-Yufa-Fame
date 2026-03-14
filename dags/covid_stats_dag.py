"""
COVID-19 Stats Pipeline DAG
============================
ETL pipeline: Ingest → Clean → Transform → Load to MySQL star-schema

Generated from prompts in: covid-19-stats-pipeline/prompts/
"""
import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add pipeline code directory to Python path
sys.path.insert(0, "/opt/airflow/pipelines/covid-19-stats-pipeline/code")

from ingest_covid_data import ingest_covid_data
from clean_covid_data import clean_covid_data
from transform_covid_data import transform_covid_data
from load_covid_model import load_happiness_model as load_covid_model

default_args = {
    "owner": "vibe-coder",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="covid_19_stats_pipeline",
    default_args=default_args,
    description="COVID-19 Global Stats ETL: Ingest → Clean → Transform → Load",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["covid", "stats", "etl", "vibe-code"],
) as dag:

    t1_ingest = PythonOperator(
        task_id="ingest_covid_data",
        python_callable=ingest_covid_data,
    )

    t2_clean = PythonOperator(
        task_id="clean_covid_data",
        python_callable=clean_covid_data,
    )

    t3_transform = PythonOperator(
        task_id="transform_covid_data",
        python_callable=transform_covid_data,
    )

    t4_load = PythonOperator(
        task_id="load_covid_model",
        python_callable=load_covid_model,
    )

    t1_ingest >> t2_clean >> t3_transform >> t4_load
