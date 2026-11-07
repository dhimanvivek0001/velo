"""Fixtures: a tiny LightGBM model registered as @champion in a temporary MLflow registry,
plus a small synthetic online store. Lets CI test the REAL API code without Databricks."""

from __future__ import annotations

import importlib

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from mlflow import MlflowClient

MODEL_NAME = "velo-demand-forecast"
FEATURES = [
    "zone_id",
    "borough",
    "hour_of_day",
    "lag_24h",
    "same_hour_4wk_avg",
    "is_holiday",
]
ZONES = {4: "Manhattan", 132: "Queens", 138: "Queens", 161: "Manhattan"}
STORE_START, STORE_DAYS = "2025-06-17", 3


def make_rows(start: str, days: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    hours = pd.date_range(start, periods=24 * days, freq="h")
    rows = []
    for zone, borough in ZONES.items():
        base = rng.uniform(20, 400)
        for ts in hours:
            level = base * (1 + 0.5 * np.sin(ts.hour / 24 * 2 * np.pi))
            rows.append(
                {
                    "zone_id": zone,
                    "hour_ts": ts,
                    "available_at": ts - pd.Timedelta(hours=24),
                    "borough": borough,
                    "hour_of_day": ts.hour,
                    "lag_24h": level * rng.normal(1, 0.1),
                    "same_hour_4wk_avg": level,
                    "is_holiday": False,
                    "materialized_at": pd.Timestamp("2025-07-01"),
                    "target": max(level * rng.normal(1, 0.05), 0),
                }
            )
    return pd.DataFrame(rows)


@pytest.fixture(scope="session")
def api_env(tmp_path_factory):
    root = tmp_path_factory.mktemp("velo")

    store = make_rows(STORE_START, STORE_DAYS, seed=1).drop(columns="target")
    store_path = root / "online_features.parquet"
    store.to_parquet(store_path)

    train = make_rows("2025-05-01", 20, seed=0)
    X = train[FEATURES].copy()
    X["zone_id"] = pd.Categorical(X["zone_id"], categories=sorted(ZONES))
    X["borough"] = pd.Categorical(X["borough"], categories=sorted(set(ZONES.values())))
    X["is_holiday"] = X["is_holiday"].astype(float)
    model = lgb.train(
        {"objective": "poisson", "num_leaves": 7, "min_data_in_leaf": 5, "verbose": -1},
        lgb.Dataset(X, train["target"], categorical_feature=["zone_id", "borough"]),
        num_boost_round=20,
    )

    tracking_uri = f"sqlite:///{(root / 'mlflow.db').as_posix()}"
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    exp_id = client.create_experiment(
        "ci", artifact_location=(root / "artifacts").as_uri()
    )
    with mlflow.start_run(experiment_id=exp_id):
        info = mlflow.lightgbm.log_model(model, name="model")
    mv = mlflow.register_model(info.model_uri, MODEL_NAME)
    client.set_model_version_tag(MODEL_NAME, mv.version, "features", ",".join(FEATURES))
    client.set_model_version_tag(MODEL_NAME, mv.version, "feature_set", "ci_fixture")
    client.set_registered_model_alias(MODEL_NAME, "champion", mv.version)
    return {"tracking_uri": tracking_uri, "store": store_path}


@pytest.fixture
def api(api_env, monkeypatch):
    """Start the real FastAPI app against the fixture registry and store."""
    monkeypatch.setenv("MLFLOW_TRACKING_URI", api_env["tracking_uri"])
    monkeypatch.setenv("VELO_ONLINE_STORE", str(api_env["store"]))
    import serving.app as app_module

    importlib.reload(app_module)  # re-read the environment variables
    with TestClient(app_module.app) as client:
        yield client, app_module
