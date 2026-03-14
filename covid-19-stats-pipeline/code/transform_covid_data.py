import pandas as pd
import numpy as np
import logging

def transform_covid_data(**context):
    clean_path = context["ti"].xcom_pull(
        task_ids="clean_covid_data", key="clean_path"
    )
    df = pd.read_csv(clean_path, parse_dates=["date"])
    df = df.sort_values(["iso_code", "date"])
    
    # 7-day rolling averages per country
    df["new_cases_7day_avg"] = (
        df.groupby("iso_code")["new_cases"]
        .transform(lambda x: x.rolling(7, min_periods=1).mean())
    )
    df["new_deaths_7day_avg"] = (
        df.groupby("iso_code")["new_deaths"]
        .transform(lambda x: x.rolling(7, min_periods=1).mean())
    )
    
    # Per-capita metrics
    df["cases_per_million"] = (df["total_cases"] / df["population"]) * 1_000_000
    df["deaths_per_million"] = (df["total_deaths"] / df["population"]) * 1_000_000
    
    # Case fatality rate (safe division)
    df["case_fatality_rate"] = np.where(
        df["total_cases"] > 0,
        (df["total_deaths"] / df["total_cases"]) * 100,
        0.0
    )
    
    # Round all float columns
    float_cols = ["new_cases_7day_avg", "new_deaths_7day_avg",
                  "cases_per_million", "deaths_per_million", "case_fatality_rate"]
    df[float_cols] = df[float_cols].round(2)
    
    logging.info(f"✓ Transform complete. Shape: {df.shape}")
    out = "/tmp/covid_transformed.csv"
    df.to_csv(out, index=False)
    context["ti"].xcom_push(key="transformed_path", value=out)
