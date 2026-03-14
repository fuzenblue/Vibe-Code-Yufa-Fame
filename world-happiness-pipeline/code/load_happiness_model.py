import pandas as pd
import logging
from sqlalchemy import create_engine
from airflow.models import Variable

def load_happiness_model(**context):
    path = context["ti"].xcom_pull(
        task_ids="clean_happiness_data", key="clean_path"
    )
    df = pd.read_csv(path)
    engine = create_engine(
        f"mysql+pymysql://{Variable.get('MYSQL_USER')}:{Variable.get('MYSQL_PASS')}"
        f"@{Variable.get('MYSQL_HOST')}/{Variable.get('MYSQL_DB')}"
    )
    
    # dim_country
    dim_country = df[["country"]].drop_duplicates().reset_index(drop=True)
    dim_country["country_id"] = dim_country.index + 1
    dim_country = dim_country.rename(columns={"country": "country_name"})
    dim_country["region"] = None
    dim_country.to_sql("dim_country", engine, if_exists="replace", index=False)
    
    # dim_year
    dim_year = df[["year"]].drop_duplicates().reset_index(drop=True)
    dim_year["year_id"] = dim_year.index + 1
    dim_year.to_sql("dim_year", engine, if_exists="replace", index=False)
    
    # Compute per-year rank
    df["happiness_rank"] = df.groupby("year")["happiness_score"].rank(
        ascending=False, method="min"
    ).astype(int)
    
    # Merge FK references
    df = df.merge(dim_country.rename(columns={"country_name":"country"}), on="country")
    df = df.merge(dim_year, on="year")
    
    fact_cols = [
        "country_id", "year_id", "happiness_score", "gdp_per_capita",
        "social_support", "life_expectancy", "freedom",
        "generosity", "corruption", "happiness_rank"
    ]
    fact = df[[c for c in fact_cols if c in df.columns]]
    fact.to_sql("fact_happiness", engine, if_exists="replace", index=False, chunksize=500)
    logging.info(f"✓ Loaded: dim_country={len(dim_country)}, dim_year={len(dim_year)}, fact={len(fact)}")
