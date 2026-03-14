import requests
import pandas as pd
import logging
from airflow.exceptions import AirflowException

def ingest_happiness_data(**context):
    sources = {
        2019: "https://raw.githubusercontent.com/rashida048/Datasets/master/world_happiness_report_2019.csv",
        2020: "https://raw.githubusercontent.com/rashida048/Datasets/master/world_happiness_report_2020.csv",
    }
    file_map = {}
    
    for year, url in sources.items():
        path = f"/tmp/happiness_{year}.csv"
        for attempt in range(3):
            try:
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                with open(path, "wb") as f:
                    f.write(r.content)
                break
            except Exception as e:
                if attempt == 2:
                    raise AirflowException(f"Failed {year}: {e}")
        
        df = pd.read_csv(path)
        logging.info(f"Year {year} columns: {list(df.columns)}")
        
        if len(df) < 100:
            raise AirflowException(f"Year {year} has only {len(df)} rows")
        
        file_map[str(year)] = path
        logging.info(f"✓ {year}: {len(df)} countries ingested")
    
    context["ti"].xcom_push(key="file_map", value=file_map)
