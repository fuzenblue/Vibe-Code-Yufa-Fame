from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from airflow.models import Variable


logger = logging.getLogger(__name__)


_UPSTREAM_TASK_ID: str = "load_taxi_model" 
_UPSTREAM_XCOM_KEY: str = "return_value" 
_DRIVER: str = "mysql+pymysql"
_CHUNK_SIZE: int = 1_000
_DIV_ZERO_SENTINEL: float = 0.0



def _extract_dataframe(ti: Any) -> pd.DataFrame:
    """
    ดึง Path จาก XCom พร้อมระบบ Error Logging ที่ละเอียดเพื่อการ Debug
    """
    csv_path: str | None = ti.xcom_pull(
        task_ids=_UPSTREAM_TASK_ID,
        key=_UPSTREAM_XCOM_KEY,
    )

    # ── Log ค่าที่ได้รับจริงก่อนตรวจสอบ ─────────────────────────────────
    logger.info(
        "[EXTRACT] XCom pull result: task_ids='%s', key='%s', value='%s'",
        _UPSTREAM_TASK_ID, _UPSTREAM_XCOM_KEY, csv_path,
    )

    if not csv_path:
        # ดึง XCom ทั้งหมดที่มีของ Task นั้นเพื่อช่วยวิเคราะห์
        all_xcom_data = ti.xcom_pull(task_ids=_UPSTREAM_TASK_ID)
        logger.error(
            "[EXTRACT] XCom Pull Failed!\n"
            "--------------------------------------------------\n"
            "Available XCom keys from '%s': %s\n"
            "Expected Key: '%s'\n"
            "Check: Ensure upstream task uses ti.xcom_push(key='%s', value=...)\n"
            "--------------------------------------------------",
            _UPSTREAM_TASK_ID, 
            list(all_xcom_data.keys()) if isinstance(all_xcom_data, dict) else "No dict data found",
            _UPSTREAM_XCOM_KEY,
            _UPSTREAM_XCOM_KEY
        )
        raise ValueError(
            f"XCom pull returned None from task '{_UPSTREAM_TASK_ID}'. "
            f"Verify upstream logic and key mapping."
        )

    if not os.path.exists(csv_path):
        logger.error("[EXTRACT] File not found on disk at: %s", csv_path)
        raise FileNotFoundError(f"CSV file missing: {csv_path}")

    logger.info("[EXTRACT] File verified. Loading into memory...")
    return pd.read_csv(csv_path, dtype=str)



def _build_engine() -> Engine:
    try:
        host = Variable.get("MYSQL_HOST")
        user = Variable.get("MYSQL_USER")
        password = Variable.get("MYSQL_PASS")
        database = Variable.get("MYSQL_DB")
        
        url = f"{_DRIVER}://{user}:{password}@{host}/{database}?charset=utf8mb4"
        engine = create_engine(url, pool_pre_ping=True)
        
        # Fast-fail check
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except KeyError as e:
        logger.error("[DB] Missing Airflow Variable: %s", e)
        raise
    except Exception as e:
        logger.error("[DB] Connection failed: %s", e)
        raise

def _safe_divide(num: pd.Series, den: pd.Series) -> pd.Series:
    return (num / den.replace(0, float("nan"))).fillna(_DIV_ZERO_SENTINEL)

def _build_dim_time(df: pd.DataFrame) -> pd.DataFrame:
    dim = df[["pickup_datetime"]].copy()
    dim["pickup_datetime"] = pd.to_datetime(dim["pickup_datetime"], errors="coerce")
    dim = dim.dropna(subset=["pickup_datetime"])
    dim["hour"] = dim["pickup_datetime"].dt.hour.astype("int8")
    dim["day"] = dim["pickup_datetime"].dt.day.astype("int8")
    dim["month"] = dim["pickup_datetime"].dt.month.astype("int8")
    dim["is_weekend"] = dim["pickup_datetime"].dt.dayofweek >= 5
    return dim.drop_duplicates(subset=["pickup_datetime"]).reset_index(drop=True)

def _build_dim_payment(df: pd.DataFrame) -> pd.DataFrame:
    return (df[["payment_type"]].copy()
            .astype(str).str.strip()
            .drop_duplicates().reset_index(drop=True))

def _build_fact_trips(df: pd.DataFrame, dim_time: pd.DataFrame, dim_payment: pd.DataFrame) -> pd.DataFrame:
    fact = df.copy()
    fact["pickup_datetime"] = pd.to_datetime(fact["pickup_datetime"], errors="coerce")
    
    for col in ["fare_amount", "trip_distance", "trip_duration_minutes"]:
        fact[col] = pd.to_numeric(fact[col], errors="coerce").fillna(0.0)
    
    fact["speed_mph"] = _safe_divide(fact["trip_distance"], fact["trip_duration_minutes"] / 60).round(4)
    fact["fare_per_mile"] = _safe_divide(fact["fare_amount"], fact["trip_distance"]).round(4)

    # Surrogate Key Mapping
    t_map = dim_time.reset_index().rename(columns={"index": "time_id"})[["time_id", "pickup_datetime"]]
    p_map = dim_payment.reset_index().rename(columns={"index": "payment_id"})[["payment_id", "payment_type"]]
    
    fact = fact.merge(t_map, on="pickup_datetime", how="left")
    fact = fact.merge(p_map, on="payment_type", how="left")
    
    return fact[["time_id", "payment_id", "fare_amount", "trip_distance", 
                 "trip_duration_minutes", "speed_mph", "fare_per_mile", "passenger_count"]]


def _load_table(df: pd.DataFrame, table_name: str, engine: Engine) -> int:
    df.to_sql(name=table_name, con=engine, if_exists="append", index=False,  #if_exists="replace".ท่า replace จะลบข้อมูลเก่าทิ้งทั้งหมด
              chunksize=_CHUNK_SIZE, method="multi")
    logger.info("[LOAD] Success: table='%s' rows=%d", table_name, len(df))
    return len(df)

def load_taxi_model(**context: Any) -> None:
    ti = context["ti"]
    
    # 1. Extract with Debug Logs
    raw_df = _extract_dataframe(ti)
    
    # 2. Connect
    engine = _build_engine()

    try:
        # 3. Transform
        logger.info("[MODEL] Transforming DataFrames...")
        dim_time = _build_dim_time(raw_df)
        dim_payment = _build_dim_payment(raw_df)
        fact_trips = _build_fact_trips(raw_df, dim_time, dim_payment)

        # 4. Load
        row_counts = {
            "dim_time": _load_table(dim_time, "dim_time", engine),
            "dim_payment": _load_table(dim_payment, "dim_payment", engine),
            "fact_trips": _load_table(fact_trips, "fact_trips", engine),
        }
        
        # 5. Final Summary Log
        logger.info("\n" + "="*30 + "\nLOAD SUMMARY\n" + "="*30)
        for tbl, cnt in row_counts.items():
            logger.info("%-15s : %d rows", tbl, cnt)
        logger.info("="*30)

    finally:
        engine.dispose()
        logger.info("[DB] Engine connection closed.")