"""Contract tests for the forecast API (run in CI against the fixture model)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from tests.conftest import FEATURES, ZONES


def test_health_reports_champion(api):
    client, _ = api
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["model_alias"] == "champion"
    assert body["n_features"] == len(FEATURES)
    assert body["store_zones"] == len(ZONES)


def test_forecast_returns_24_point_in_time_hours(api):
    client, _ = api
    r = client.get("/forecast/138", params={"date": "2025-06-18"})
    assert r.status_code == 200
    body = r.json()
    hours = body["hours"]
    assert len(hours) == 24
    assert all(h["predicted_trips"] >= 0 for h in hours)
    for h in hours:  # every feature row was known 24h before the hour it forecasts
        gap = pd.Timestamp(h["hour_ts"]) - pd.Timestamp(h["available_at"])
        assert gap == pd.Timedelta(hours=24)
    assert (
        abs(body["total_predicted_trips"] - sum(h["predicted_trips"] for h in hours))
        < 1.0
    )


def test_unknown_zone_is_404(api):
    client, _ = api
    assert client.get("/forecast/999", params={"date": "2025-06-18"}).status_code == 404


def test_date_outside_store_is_422(api):
    client, _ = api
    r = client.get("/forecast/138", params={"date": "2025-01-01"})
    assert r.status_code == 422
    assert "Online store covers" in r.json()["detail"]


def test_bad_date_format_is_422(api):
    client, _ = api
    assert client.get("/forecast/138", params={"date": "18-06-2025"}).status_code == 422


def test_categories_come_from_model_not_request(api):
    """Skew guard: a one-zone request must still use the model's FULL category list."""
    _, app_module = api
    state = app_module.state
    rows = state["store"][state["store"]["zone_id"] == 138]
    X = app_module.build_matrix(rows, state["features"], state["model"])
    assert list(X["zone_id"].cat.categories) == sorted(ZONES)


def test_prediction_independent_of_request_batch(api):
    """A zone's forecast must not change depending on which other zones are in the request."""
    _, app_module = api
    state = app_module.state
    store, model, features = state["store"], state["model"], state["features"]
    alone = store[store["zone_id"] == 138]
    pred_alone = model.predict(app_module.build_matrix(alone, features, model))
    pred_all = model.predict(app_module.build_matrix(store, features, model))
    pred_in_batch = pred_all[(store["zone_id"] == 138).to_numpy()]
    assert np.allclose(pred_alone, pred_in_batch)
