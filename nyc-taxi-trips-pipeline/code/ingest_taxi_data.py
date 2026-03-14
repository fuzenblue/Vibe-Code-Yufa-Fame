import logging
import os
import time

import pandas as pd
import requests
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

# ==================== Configuration ====================
API_URL = "https://data.cityofnewyork.us/resource/t29m-gskq.csv"
ROW_LIMIT = 1000
MIN_EXPECTED_ROWS = 100
MAX_RETRIES = 3
RETRY_DELAY_SEC = 5
OUTPUT_FILENAME = "nyc_taxi_raw.csv"


def _get_data_dir():
    """Return the pipeline's data/ directory (creating it if needed)."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def ingest_taxi_data(**context):
    """
    Download NYC Yellow Taxi Trip Records via the Socrata SODA API,
    save locally, validate, and push file path to XCom.
    """

    params = {"$limit": ROW_LIMIT, "$order": ":id"}
    output_path = os.path.join(_get_data_dir(), OUTPUT_FILENAME)

    # ---------- Download with retry ----------
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Attempt %d/%d — downloading from %s", attempt, MAX_RETRIES, API_URL)

            resp = requests.get(API_URL, params=params, stream=True, timeout=120)
            resp.raise_for_status()

            bytes_written = 0
            with open(output_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        bytes_written += len(chunk)

            log.info("Saved %s (%d bytes)", output_path, bytes_written)
            break

        except requests.exceptions.RequestException as exc:
            log.warning("Attempt %d failed: %s", attempt, exc)
            if attempt == MAX_RETRIES:
                raise AirflowException(
                    f"Download failed after {MAX_RETRIES} attempts: {exc}"
                ) from exc
            time.sleep(RETRY_DELAY_SEC)

    # ---------- Lightweight row-count validation ----------
    # Count rows without loading full DataFrame into memory
    with open(output_path, "r", encoding="utf-8") as f:
        row_count = sum(1 for _ in f) - 1  # subtract header

    if row_count < MIN_EXPECTED_ROWS:
        raise AirflowException(
            f"Validation failed: expected >= {MIN_EXPECTED_ROWS} rows, got {row_count}"
        )

    log.info("✓ Row count validation passed: %d rows", row_count)

    # ---------- Push to XCom ----------
    context["ti"].xcom_push(key="taxi_raw_csv_path", value=output_path)
    return output_path