import pandas as pd
import logging

LOAD_COLS = [
    "iso_code", "location", "date",
    "new_cases", "new_deaths",
    "total_cases", "total_deaths", "population"
]

def clean_covid_data(**context):
    raw_path = context["ti"].xcom_pull(
        task_ids="ingest_covid_data", key="raw_path"
    )
    df = pd.read_csv(raw_path, usecols=LOAD_COLS, parse_dates=["date"])
    n0 = len(df)
    logging.info(f"Loaded {n0:,} rows")
    
    # Remove OWID aggregate regions (continents, income groups)
    df = df[~df["iso_code"].str.startswith("OWID_", na=False)]
    logging.info(f"After OWID filter: {len(df):,} rows")
    
    # Clip negative correction values to 0
    df["new_cases"] = df["new_cases"].clip(lower=0).fillna(0)
    df["new_deaths"] = df["new_deaths"].clip(lower=0).fillna(0)
    
    # Remove rows with invalid population
    df = df[(df["population"].notna()) & (df["population"] > 0)]
    
    # Remove duplicates
    df = df.drop_duplicates(subset=["iso_code", "date"], keep="first")
    
    logging.info(f"✓ Clean rows: {len(df):,} | Countries: {df['iso_code'].nunique()}")
    out = "/tmp/covid_clean.csv"
    df.to_csv(out, index=False)
    context["ti"].xcom_push(key="clean_path", value=out)
