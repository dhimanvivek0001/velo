"""Phase 9.2b: FastAPI service for the day-ahead demand forecast.

- Model: loaded from the MLflow Model Registry by ALIAS (models:/velo-demand-forecast@champion).
  Promoting or rolling back = moving the alias and restarting; no code change.
- Features: read from the ONLINE feature store (serving/materialize_online.py), built with the
  same joins as the training set, so training and serving see identical inputs.
- Skew guard: categorical codes come from the MODEL, not from the request data.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Annotated

import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from mlflow import MlflowClient
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODEL_NAME = os.getenv("VELO_MODEL_NAME", "velo-demand-forecast")
MODEL_ALIAS = os.getenv("VELO_MODEL_ALIAS", "champion")
TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI", f"sqlite:///{(PROJECT_ROOT / 'mlflow.db').as_posix()}"
)
ONLINE_STORE = Path(
    os.getenv(
        "VELO_ONLINE_STORE",
        str(PROJECT_ROOT / "data" / "online" / "online_features.parquet"),
    )
)
CATEGORICAL = ("zone_id", "borough")

state: dict = {}


def load_online_store(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise RuntimeError(
            f"Online store not found at {path}. Run serving/materialize_online.py"
        )
    df = pd.read_parquet(path)
    for col in ("hour_ts", "available_at", "materialized_at"):
        ts = pd.to_datetime(df[col])
        if ts.dt.tz is not None:
            ts = ts.dt.tz_localize(None)
        df[col] = ts
    # Databricks DECIMAL columns and nullable flags arrive as Python objects: make them float
    for col in df.columns:
        if df[col].dtype == object and col != "borough":
            df[col] = df[col].astype(float)
    return df


def build_matrix(rows: pd.DataFrame, features: list[str], model) -> pd.DataFrame:
    """Serving-side model matrix. Category lists are taken from the trained model,
    so codes always match training whatever subset of zones a request contains."""
    X = rows[features].copy()
    cat_cols = [c for c in features if c in CATEGORICAL]
    model_cats = model.pandas_categorical or []
    if len(model_cats) != len(cat_cols):
        raise RuntimeError(
            f"Model has {len(model_cats)} categorical lists, expected {len(cat_cols)}"
        )
    for col, cats in zip(cat_cols, model_cats, strict=True):
        X[col] = pd.Categorical(X[col], categories=cats)
    for col in features:
        if col not in cat_cols:
            X[col] = X[col].astype(float)
    return X


@asynccontextmanager
async def lifespan(_app: FastAPI):
    mlflow.set_tracking_uri(TRACKING_URI)
    client = MlflowClient()
    mv = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
    state["model"] = mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}@{MODEL_ALIAS}")
    state["version"] = str(mv.version)
    state["features"] = mv.tags["features"].split(",")
    state["feature_set"] = mv.tags.get("feature_set", "")
    state["store"] = load_online_store(ONLINE_STORE)
    missing = set(state["features"]) - set(state["store"].columns)
    if missing:
        raise RuntimeError(f"Online store is missing model features: {sorted(missing)}")
    yield
    state.clear()


app = FastAPI(
    title="Velo demand forecast API",
    description="Day-ahead trips forecast per NYC pickup zone and hour (LightGBM, @champion).",
    version="1.0.0",
    lifespan=lifespan,
)


class HourForecast(BaseModel):
    hour_ts: datetime
    available_at: datetime
    predicted_trips: float


class ForecastResponse(BaseModel):
    zone_id: int
    borough: str
    forecast_date: date
    model_name: str
    model_alias: str
    model_version: str
    feature_set: str
    total_predicted_trips: float
    hours: list[HourForecast]


class Health(BaseModel):
    status: str
    model_name: str
    model_alias: str
    model_version: str
    n_features: int
    store_rows: int
    store_zones: int
    store_from: datetime
    store_to: datetime
    store_materialized_at: datetime


@app.get("/health", response_model=Health)
def health() -> Health:
    store = state["store"]
    return Health(
        status="ok",
        model_name=MODEL_NAME,
        model_alias=MODEL_ALIAS,
        model_version=state["version"],
        n_features=len(state["features"]),
        store_rows=len(store),
        store_zones=int(store["zone_id"].nunique()),
        store_from=store["hour_ts"].min().to_pydatetime(),
        store_to=store["hour_ts"].max().to_pydatetime(),
        store_materialized_at=store["materialized_at"].max().to_pydatetime(),
    )


@app.get("/forecast/{zone_id}", response_model=ForecastResponse)
def forecast(
    zone_id: int,
    forecast_date: Annotated[
        date, Query(alias="date", description="Day to forecast, YYYY-MM-DD")
    ],
) -> ForecastResponse:
    store = state["store"]
    day = pd.Timestamp(forecast_date)
    rows = store[
        (store["zone_id"] == zone_id)
        & (store["hour_ts"] >= day)
        & (store["hour_ts"] < day + pd.Timedelta(days=1))
    ].sort_values("hour_ts")

    if rows.empty:
        if zone_id not in set(store["zone_id"]):
            raise HTTPException(status_code=404, detail=f"Unknown zone_id {zone_id}")
        raise HTTPException(
            status_code=422,
            detail=(
                f"No features for {forecast_date}. Online store covers "
                f"{store['hour_ts'].min():%Y-%m-%d} to {store['hour_ts'].max():%Y-%m-%d}"
            ),
        )

    X = build_matrix(rows, state["features"], state["model"])
    pred = np.clip(state["model"].predict(X), 0, None)
    hours = [
        HourForecast(
            hour_ts=h.to_pydatetime(),
            available_at=a.to_pydatetime(),
            predicted_trips=round(float(p), 1),
        )
        for h, a, p in zip(rows["hour_ts"], rows["available_at"], pred, strict=True)
    ]
    return ForecastResponse(
        zone_id=zone_id,
        borough=str(rows["borough"].iloc[0]),
        forecast_date=forecast_date,
        model_name=MODEL_NAME,
        model_alias=MODEL_ALIAS,
        model_version=state["version"],
        feature_set=state["feature_set"],
        total_predicted_trips=round(float(pred.sum()), 1),
        hours=hours,
    )
