import pandas as pd
import logging

def clean_happiness_data(**context):
    path = context["ti"].xcom_pull(
        task_ids="transform_happiness_data", key="combined_path"
    )
    df = pd.read_csv(path)
    n0 = len(df)
    
    # Validate score range
    df = df[(df["happiness_score"].notna()) &
            df["happiness_score"].between(0, 10)]
    logging.info(f"After score filter: {len(df)} rows (removed {n0-len(df)})")
    
    # Standardize country names
    df["country"] = df["country"].str.strip().str.title()
    
    # IQR outlier removal on happiness_score
    Q1 = df["happiness_score"].quantile(0.25)
    Q3 = df["happiness_score"].quantile(0.75)
    IQR = Q3 - Q1
    before = len(df)
    df = df[df["happiness_score"].between(Q1 - 1.5*IQR, Q3 + 1.5*IQR)]
    logging.info(f"IQR filter removed {before-len(df)} outliers")
    
    # Fill numeric nulls with year median
    numeric_cols = ["gdp_per_capita", "social_support", "freedom", "generosity"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df.groupby("year")[col].transform(
                lambda x: x.fillna(x.median())
            )
    
    # Remove duplicates
    df = df.drop_duplicates(subset=["country", "year"], keep="first")
    logging.info(f"✓ Final clean dataset: {len(df)} rows")
    
    out = "/tmp/happiness_clean.csv"
    df.to_csv(out, index=False)
    context["ti"].xcom_push(key="clean_path", value=out)
