import pandas as pd
import logging

COLUMN_MAP = {
    "Country or region": "country",
    "Country name": "country",
    "Score": "happiness_score",
    "Ladder score": "happiness_score",
    "GDP per capita": "gdp_per_capita",
    "Logged GDP per capita": "gdp_per_capita",
    "Social support": "social_support",
    "Healthy life expectancy": "life_expectancy",
    "Healthy life expectancy at birth": "life_expectancy",
    "Freedom to make life choices": "freedom",
    "Generosity": "generosity",
    "Perceptions of corruption": "corruption",
}

def transform_happiness_data(**context):
    file_map = context["ti"].xcom_pull(
        task_ids="ingest_happiness_data", key="file_map"
    )
    dfs = []
    for year, path in file_map.items():
        df = pd.read_csv(path)
        df = df.rename(columns=COLUMN_MAP)
        df["year"] = int(year)
        keep = ["country", "year", "happiness_score",
                "gdp_per_capita", "social_support",
                "life_expectancy", "freedom", "generosity", "corruption"]
        df = df[[c for c in keep if c in df.columns]]
        dfs.append(df)
    
    combined = pd.concat(dfs, ignore_index=True)
    logging.info(f"Combined shape: {combined.shape}")
    logging.info(combined.head(3).to_string())
    
    out = "/tmp/happiness_combined.csv"
    combined.to_csv(out, index=False)
    context["ti"].xcom_push(key="combined_path", value=out)
