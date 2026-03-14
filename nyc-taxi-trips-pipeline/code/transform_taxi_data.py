from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

_UPSTREAM_TASK_ID: str = "transform_taxi_data"
_UPSTREAM_XCOM_KEY: str = "return_value"
_OUTPUT_PATH: str = "/tmp/nyc_taxi_transformed.csv"
_SAMPLE_ROWS: int = 3
_DURATION_MAX_MINUTES: float = 240.0  # ✅ เพิ่ม upper-bound guard

_COLUMN_RENAME_MAP: dict[str, str] = {
    "tpep_pickup_datetime":  "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "PULocationID":          "pickup_location_id",
    "DOLocationID":          "dropoff_location_id",
    "RatecodeID":            "rate_code_id",
    "VendorID":              "vendor_id",
}

_MEASURE_COLS: tuple[str, ...] = (
    "fare_amount", "trip_distance", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge",
)


@dataclass
class TransformReport:
    input_rows: int = 0
    rows_after_duration: int = 0
    final_rows: int = 0
    negative_duration_rows: int = 0
    nat_duration_rows: int = 0
    excessive_duration_rows: int = 0        # ✅ เพิ่ม field ใหม่
    columns_renamed: list[tuple[str, str]] = field(default_factory=list)
    measures_cast: list[str] = field(default_factory=list)
    output_path: str = ""
    warnings: list[str] = field(default_factory=list)

    @property
    def rows_removed(self) -> int:
        return self.input_rows - self.final_rows

    @property
    def retention_pct(self) -> float:
        if self.input_rows == 0:
            return 0.0
        return round(self.final_rows / self.input_rows * 100, 2)


def _extract_dataframe(ti: Any) -> pd.DataFrame:
    logger.info(
        "[EXTRACT] Pulling XCom → task_ids='%s', key='%s'",
        _UPSTREAM_TASK_ID, _UPSTREAM_XCOM_KEY,
    )

    csv_path: str | None = ti.xcom_pull(
        task_ids=_UPSTREAM_TASK_ID,
        key=_UPSTREAM_XCOM_KEY,
    )

    if not csv_path:
        fallback: str | None = ti.xcom_pull(task_ids=_UPSTREAM_TASK_ID)
        logger.error(
            "[EXTRACT] XCom pull returned None.\n"
            "  Searched → task_ids='%s', key='%s'\n"
            "  Fallback → key=None → value='%s'\n"
            "  Fix      → ensure clean_taxi_data returns the CSV path.",
            _UPSTREAM_TASK_ID, _UPSTREAM_XCOM_KEY, fallback,
        )
        raise ValueError(
            f"XCom pull failed: task_ids='{_UPSTREAM_TASK_ID}', "
            f"key='{_UPSTREAM_XCOM_KEY}' returned None. "
            f"Fallback value: '{fallback}'."
        )

    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Cleaned CSV path from XCom not found on disk: '{csv_path}'."
        )

    logger.info("[EXTRACT] Reading: %s", csv_path)
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)

    if df.empty:
        raise pd.errors.EmptyDataError(f"CSV at '{csv_path}' is empty.")

    logger.info("[EXTRACT] Loaded %d rows × %d columns.", *df.shape)
    return df


def _rename_columns(df: pd.DataFrame, report: TransformReport) -> pd.DataFrame:
    applicable_map = {
        src: tgt for src, tgt in _COLUMN_RENAME_MAP.items()
        if src in df.columns
    }
    skipped = set(_COLUMN_RENAME_MAP) - set(applicable_map)
    if skipped:
        logger.warning("[RENAME] Skipping absent columns: %s", sorted(skipped))

    df = df.rename(columns=applicable_map)
    report.columns_renamed = list(applicable_map.items())
    logger.info("[RENAME] Renamed %d column(s).", len(applicable_map))
    return df


def _cast_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("pickup_datetime", "dropoff_datetime"):
        if col not in df.columns:
            logger.warning("[CAST] '%s' not found — skipping.", col)
            continue

        before_nat = df[col].isna().sum()
        df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
        new_nats = df[col].isna().sum() - before_nat

        if new_nats > 0:
            logger.warning("[CAST] '%s': %d new NaT(s) from parse.", col, new_nats)

    logger.info("[CAST] Datetime columns parsed.")
    return df


def _cast_measure_columns(df: pd.DataFrame, report: TransformReport) -> pd.DataFrame:
    cast_ok: list[str] = []
    for col in _MEASURE_COLS:
        if col not in df.columns:
            logger.warning("[CAST] Measure '%s' not found — skipping.", col)
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        cast_ok.append(col)

    report.measures_cast = cast_ok
    logger.info("[CAST] %d measure(s) cast to float64.", len(cast_ok))
    return df


def _cast_payment_type(df: pd.DataFrame) -> pd.DataFrame:
    if "payment_type" not in df.columns:
        logger.warning("[CAST] 'payment_type' not found — skipping.")
        return df

    # ✅ FIX: handle "nan" / "None" before split to avoid phantom dim_payment keys
    df["payment_type"] = (
        df["payment_type"]
        .astype(str)
        .str.strip()
        .replace({"nan": None, "None": None, "": None})
        .str.split(".")
        .str[0]
    )

    null_count = df["payment_type"].isna().sum()
    if null_count:
        logger.warning(
            "[CAST] %d null payment_type value(s) after normalisation.", null_count
        )

    value_counts = df["payment_type"].value_counts(dropna=False).to_dict()
    logger.info("[CAST] payment_type distribution: %s", value_counts)
    return df


def _engineer_trip_duration(
    df: pd.DataFrame,
    report: TransformReport,
    strategy: str = "zero",
) -> pd.DataFrame:
    if "pickup_datetime" not in df.columns or "dropoff_datetime" not in df.columns:
        logger.warning("[DURATION] Missing datetime columns — set all to 0.0.")
        df["trip_duration_minutes"] = 0.0
        report.rows_after_duration = len(df)
        return df

    raw_delta = df["dropoff_datetime"] - df["pickup_datetime"]
    df["trip_duration_minutes"] = raw_delta.dt.total_seconds() / 60

    # ✅ FIX: fill NaT FIRST, then compute neg_mask on clean float column
    nat_mask = df["trip_duration_minutes"].isna()
    report.nat_duration_rows = int(nat_mask.sum())
    if report.nat_duration_rows:
        logger.warning(
            "[DURATION] %d NaT duration(s) → set to 0.0.", report.nat_duration_rows
        )
        df["trip_duration_minutes"] = df["trip_duration_minutes"].fillna(0.0)

    # neg_mask is now safe — no NaN in series
    neg_mask = df["trip_duration_minutes"] < 0
    report.negative_duration_rows = int(neg_mask.sum())
    if report.negative_duration_rows:
        logger.warning(
            "[DURATION] %d negative duration(s) (strategy='%s').",
            report.negative_duration_rows, strategy,
        )
        if strategy == "drop":
            df = df[~neg_mask].reset_index(drop=True)
        else:
            df.loc[neg_mask, "trip_duration_minutes"] = 0.0

    # ✅ NEW: upper-bound guard — flag trips longer than 4 hours
    excess_mask = df["trip_duration_minutes"] > _DURATION_MAX_MINUTES
    report.excessive_duration_rows = int(excess_mask.sum())
    if report.excessive_duration_rows:
        logger.warning(
            "[DURATION] %d row(s) exceed %.0f min ceiling — capping at %.0f.",
            report.excessive_duration_rows,
            _DURATION_MAX_MINUTES,
            _DURATION_MAX_MINUTES,
        )
        df.loc[excess_mask, "trip_duration_minutes"] = _DURATION_MAX_MINUTES

    df["trip_duration_minutes"] = df["trip_duration_minutes"].round(4)
    report.rows_after_duration = len(df)

    logger.info(
        "[DURATION] Engineered. min=%.2f  max=%.2f  mean=%.2f  median=%.2f",
        df["trip_duration_minutes"].min(),
        df["trip_duration_minutes"].max(),
        df["trip_duration_minutes"].mean(),
        df["trip_duration_minutes"].median(),
    )
    return df


def _log_sample_transformations(df: pd.DataFrame, n: int = _SAMPLE_ROWS) -> None:
    sample_cols = [
        col for col in (
            "pickup_datetime", "dropoff_datetime",
            "trip_duration_minutes", "fare_amount",
            "trip_distance", "payment_type",
        )
        if col in df.columns
    ]
    sample = df[sample_cols].head(n)
    bar = "─" * 72
    logger.info("\n%s\n  Sample (first %d rows)\n%s", bar, n, bar)
    for idx, row in sample.iterrows():
        logger.info("  Row %d:", idx)
        for col in sample_cols:
            logger.info("    %-30s  %s", col, row[col])
        logger.info(bar)


def _apply_transformations(
    df: pd.DataFrame,
    duration_strategy: str = "zero",      # ✅ strategy วิ่งผ่าน parameter แทน global
) -> tuple[pd.DataFrame, TransformReport]:
    report = TransformReport(input_rows=len(df))
    logger.info("[TRANSFORM] Starting on %d rows.", report.input_rows)

    df = _rename_columns(df, report)
    df = _cast_datetime_columns(df)
    df = _cast_measure_columns(df, report)
    df = _cast_payment_type(df)
    df = _engineer_trip_duration(df, report, strategy=duration_strategy)
    _log_sample_transformations(df)

    report.final_rows = len(df)
    return df, report


def _save_transformed_csv(df: pd.DataFrame, output_path: str) -> str:
    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    df.to_csv(output_path, index=False)
    abs_path = os.path.abspath(output_path)
    size_kb = os.path.getsize(abs_path) / 1024
    logger.info(
        "[OUTPUT] Saved → %s  (%.1f KB, %d rows, %d cols)",
        abs_path, size_kb, len(df), len(df.columns),
    )
    return abs_path


def _log_transform_report(report: TransformReport) -> None:
    w = 64
    heavy, light = "=" * w, "-" * w

    def _row(label: str, value: str) -> None:
        logger.info("| %-38s ->  %-20s|", label, value)

    logger.info("+%s+", heavy)
    logger.info("|%s|", "  Transformation Pipeline Summary".center(w))
    logger.info("+%s+", heavy)
    _row("Input rows", f"{report.input_rows:>12,}")
    logger.info("+%s+", light)
    _row("Columns renamed",              f"{len(report.columns_renamed):>12,}")
    _row("Measures cast to float64",     f"{len(report.measures_cast):>12,}")
    _row("Rows with NaT duration",       f"{report.nat_duration_rows:>12,}")
    _row("Rows with negative duration",  f"{report.negative_duration_rows:>12,}")
    _row("Rows exceeding duration cap",  f"{report.excessive_duration_rows:>12,}")
    logger.info("+%s+", light)
    _row("Final rows",      f"{report.final_rows:>12,}")
    _row("Rows removed",    f"{report.rows_removed:>12,}")
    _row("Retention rate",  f"{report.retention_pct:>11.2f}%")
    _row("Output path",     report.output_path)
    if report.warnings:
        logger.info("+%s+", light)
        for w_msg in report.warnings:
            logger.info("| WARN: %-57s|", w_msg[:57])
    logger.info("+%s+", heavy)


def transform_taxi_data(**context: Any) -> str:
    ti: Any = context["ti"]

    # ── runtime config via op_kwargs (overrides default) ──────────────────
    duration_strategy: str = context.get("duration_strategy", "zero")

    cleaned_df = _extract_dataframe(ti)
    transformed_df, report = _apply_transformations(
        cleaned_df, duration_strategy=duration_strategy
    )
    abs_path = _save_transformed_csv(transformed_df, _OUTPUT_PATH)
    report.output_path = abs_path
    _log_transform_report(report)

    logger.info(
        "[DONE] %d/%d rows retained (%.2f%%). XCom path: %s",
        report.final_rows, report.input_rows, report.retention_pct, abs_path,
    )
    return abs_path