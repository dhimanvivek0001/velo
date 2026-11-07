"""Shared helpers for the Phase 7 day-ahead demand forecasting models."""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from databricks.sdk.core import Config
from dotenv import load_dotenv

from databricks import sql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
DATA_FILE = PROJECT_ROOT / "data" / "ml" / "ml_zone_hour_features.parquet"

# Time-based split: train on the past, validate and test on the future
TRAIN_END = pd.Timestamp("2025-03-01")  # train:      29 Jan 2024 - 28 Feb 2025
VALID_END = pd.Timestamp("2025-05-01")  # validation: Mar - Apr 2025
#                                          test:       May - Jun 2025 (touched once, at the end)


def load_features(refresh: bool = False) -> pd.DataFrame:
    """Feature table from dbt, cached locally as Parquet (download once, ~3.3M rows)."""
    if not DATA_FILE.exists() or refresh:
        cfg = Config(profile=os.environ.get("DATABRICKS_PROFILE", "velo"))
        print("Downloading features from Databricks (one-time, a few minutes)...")
        with (
            sql.connect(
                server_hostname=cfg.host.replace("https://", ""),
                http_path=os.environ["DATABRICKS_HTTP_PATH"],
                credentials_provider=lambda: cfg.authenticate,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("select * from velo.gold.ml_zone_hour_features")
            table = cur.fetchall_arrow()
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, DATA_FILE)

    df = pd.read_parquet(DATA_FILE)
    ts = pd.to_datetime(df["hour_ts"])
    if ts.dt.tz is not None:  # keep NYC wall-clock time, drop the timezone label
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    df["hour_ts"] = ts
    # Databricks DECIMAL columns (and nullable flags) arrive as Python objects: make them float
    for col in df.columns.drop(["borough", "date_day"]):
        if df[col].dtype == object:
            df[col] = df[col].astype(float)
    return df.sort_values(["hour_ts", "zone_id"]).reset_index(drop=True)


def split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train = df[df["hour_ts"] < TRAIN_END]
    valid = df[(df["hour_ts"] >= TRAIN_END) & (df["hour_ts"] < VALID_END)]
    test = df[df["hour_ts"] >= VALID_END]
    return train, valid, test


def metrics(y_true, y_pred) -> dict[str, float]:
    y_true = np.asarray(y_true, dtype=float)
    err = np.asarray(y_pred, dtype=float) - y_true
    return {
        "MAE": float(np.abs(err).mean()),
        "RMSE": float(np.sqrt((err**2).mean())),
        "WAPE_pct": float(np.abs(err).sum() / y_true.sum() * 100),
        "Bias_pct": float(err.sum() / y_true.sum() * 100),
    }
