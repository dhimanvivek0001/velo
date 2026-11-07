"""Training-serving skew test (local only: needs the real data).

The online store (what the API sees) must contain exactly the same feature values as the
training set (what the model learned from) for the same zone-hours."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ONLINE = PROJECT_ROOT / "data" / "online" / "online_features.parquet"
TRAINING = PROJECT_ROOT / "data" / "ml" / "ml_training_set.parquet"
KEYS = ["zone_id", "hour_ts"]
NOT_FEATURES = {"available_at", "materialized_at", "prediction_ts", "date_day", "trips"}

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (ONLINE.exists() and TRAINING.exists()),
        reason="local feature data not available",
    ),
]


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("hour_ts", "available_at", "prediction_ts", "materialized_at"):
        if col in df.columns:
            ts = pd.to_datetime(df[col])
            if ts.dt.tz is not None:
                ts = ts.dt.tz_localize(None)
            df[col] = ts
    for col in df.columns:
        if df[col].dtype == object and col not in ("borough", "date_day"):
            df[col] = df[col].astype(float)
    return df


def test_online_store_matches_training_set():
    online = _normalise(pd.read_parquet(ONLINE))
    train = _normalise(pd.read_parquet(TRAINING))
    train = train[
        train["hour_ts"].between(online["hour_ts"].min(), online["hour_ts"].max())
    ]

    merged = online.merge(
        train, on=KEYS, suffixes=("_online", "_train"), validate="one_to_one"
    )
    assert len(merged) == len(online), "some online rows have no matching training row"

    shared = [
        c
        for c in online.columns
        if c in train.columns and c not in set(KEYS) | NOT_FEATURES
    ]
    mismatches = {}
    for col in shared:
        a, b = merged[f"{col}_online"], merged[f"{col}_train"]
        if col == "borough":
            bad = int((a != b).sum())
        else:
            bad = int(
                (~np.isclose(a.astype(float), b.astype(float), equal_nan=True)).sum()
            )
        if bad:
            mismatches[col] = bad
    assert not mismatches, (
        f"Training-serving skew (column: mismatched rows): {mismatches}"
    )
    print(
        f"\n{len(shared)} features x {len(merged):,} zone-hours identical online vs training"
    )
