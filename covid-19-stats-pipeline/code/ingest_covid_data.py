import requests
import pandas as pd
import logging
from airflow.exceptions import AirflowException

REQUIRED_COLS = [
    "iso_code", "location", "date",
    "total_cases", "new_cases",
    "total_deaths", "new_deaths", "population"
]

def ingest_covid_data(**context):
    url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    output_path = "/tmp/covid_raw.csv"
    
    for attempt in range(3):
        try:
            logging.info(f"Downloading OWID COVID-19 data (attempt {attempt+1})...")
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(output_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
            break
        except Exception as e:
            if attempt == 2:
                raise AirflowException(f"Download failed: {e}")
    
    df = pd.read_csv(output_path, nrows=5)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise AirflowException(f"Missing columns: {missing}")
    
    # Stats on full file (memory-efficient)
    full = pd.read_csv(output_path, usecols=["iso_code", "date"])
    logging.info(f"Countries: {full['iso_code'].nunique()}")
    logging.info(f"Date range: {full['date'].min()} → {full['date'].max()}")
    logging.info(f"Total rows: {len(full):,}")
    
    context["ti"].xcom_push(key="raw_path", value=output_path)
