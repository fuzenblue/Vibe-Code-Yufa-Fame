import logging
import os

import pandas as pd

log = logging.getLogger(__name__)

# ==================== Cleaning thresholds ====================
FARE_MIN, FARE_MAX = 0, 500             # USD
DISTANCE_MIN, DISTANCE_MAX = 0, 100     # miles
CRITICAL_COLUMNS = ["fare_amount", "trip_distance", "tpep_pickup_datetime"]
OUTPUT_FILENAME = "nyc_taxi_clean.csv" # save cleaned data to this file


def _get_data_dir():
    """Return the pipeline's data/ directory (creating it if needed)."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def _log_step(step_name: str, before: int, after: int):
    """Log how many rows were removed in a cleaning step."""
    removed = before - after
    log.info("  %-30s  %6d → %6d  (-%d)", step_name, before, after, removed)


def clean_taxi_data(**context):
    """
    Load raw NYC taxi CSV, apply cleaning rules on fare, distance,
    and null values, then save the cleaned dataset.
    """

    # 1. Pull raw CSV path from XCom
    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="ingest_taxi_data", key="taxi_raw_csv_path")

    if not raw_path or not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw CSV not found: {raw_path}")

    # 2. Load CSV — parse datetime at read-time for better performance
    log.info("Loading raw data from: %s", raw_path)
    df = pd.read_csv(raw_path, parse_dates=["tpep_pickup_datetime"])
    initial = len(df)
    log.info("Initial rows: %d", initial)

    # 3. Apply cleaning rules (each step logged)
    log.info("Applying cleaning rules:")

    # Drop nulls in critical columns
    existing_critical = [c for c in CRITICAL_COLUMNS if c in df.columns]
    before = len(df)
    df = df.dropna(subset=existing_critical)
    _log_step("Drop nulls", before, len(df))

    # Fare amount filter
    if "fare_amount" in df.columns:
        before = len(df)
        df = df[df["fare_amount"].between(FARE_MIN, FARE_MAX, inclusive="neither")]
        _log_step(f"Fare ({FARE_MIN}-{FARE_MAX})", before, len(df))

    # Trip distance filter
    if "trip_distance" in df.columns:
        before = len(df)
        df = df[df["trip_distance"].between(DISTANCE_MIN, DISTANCE_MAX, inclusive="neither")]
        _log_step(f"Distance ({DISTANCE_MIN}-{DISTANCE_MAX} mi)", before, len(df))

    # Note: Coordinate filter skipped — dataset uses PULocationID/DOLocationID

    # 4. Summary
    final = len(df)
    removed = initial - final
    pct = (removed / initial * 100) if initial > 0 else 0
    log.info("✓ Cleaning complete: %d → %d rows (removed %d, %.1f%%)", initial, final, removed, pct)

    # 5. Save cleaned data
    clean_path = os.path.join(_get_data_dir(), OUTPUT_FILENAME)
    df.to_csv(clean_path, index=False)
    log.info("Saved cleaned data to: %s", clean_path)

    # 6. Push to XCom
    ti.xcom_push(key="taxi_clean_csv_path", value=clean_path)
    return clean_path
