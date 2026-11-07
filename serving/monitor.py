"""Phase 9.4: model monitoring - accuracy against actuals, feature drift, feed completeness.

Runs daily once actuals land (here: a replay over the online-store window, 17-30 Jun 2025).
- Accuracy: champion WAPE per day vs the 4-week-average baseline. ALERT if the 3-day rolling
  WAPE is worse than the WAPE measured at training time by more than ACCURACY_TOLERANCE.
- Drift (PSI, training period vs serving window):
    * DEMAND features should look like training -> ALERT if PSI > PSI_ALERT.
    * SEASONAL inputs (weather) move with the calendar by design -> reported, never alerted:
      a 2-week window can never match a year-round reference (lesson from the first run).
- Completeness: ALERT if any model feature's missing rate in the serving window exceeds
  NULL_ALERT (catches a broken feed, e.g. the weather API stopping).
Exit code 1 when an alert fires, so a scheduler (e.g. Airflow) marks the run failed.
"""

from __future__ import annotations

import sys

import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
from mlflow import MlflowClient

from serving.app import (
    MODEL_ALIAS,
    MODEL_NAME,
    ONLINE_STORE,
    PROJECT_ROOT,
    TRACKING_URI,
    build_matrix,
    load_online_store,
)

TRAINING = PROJECT_ROOT / "data" / "ml" / "ml_training_set.parquet"
OUT_DIR = PROJECT_ROOT / "data" / "monitoring"
TRAIN_END = pd.Timestamp("2025-03-01")
ACCURACY_TOLERANCE = (
    1.25  # alert if rolling WAPE > 1.25 x the WAPE measured at training time
)
PSI_ALERT = 0.25
PSI_FLOOR = (
    1e-4  # smoothing for empty bins (1e-6 lets a single empty bin explode the score)
)
NULL_ALERT = 0.05  # alert if more than 5% of a feature is missing in the serving window
DEMAND_FEATURES = ["lag_24h", "lag_168h", "same_hour_4wk_avg", "avg_prev_7d"]
SEASONAL_FEATURES = ["temp_c", "precip_mm"]


def psi(expected: pd.Series, actual: pd.Series, bins: int = 10) -> float:
    """Population Stability Index with quantile bins taken from the reference period."""
    e, a = expected.dropna().to_numpy(), actual.dropna().to_numpy()
    if len(e) == 0 or len(a) == 0:
        return float("nan")
    edges = np.unique(np.quantile(e, np.linspace(0, 1, bins + 1)))
    edges[0], edges[-1] = -np.inf, np.inf
    e_pct = np.clip(np.histogram(e, edges)[0] / len(e), PSI_FLOOR, None)
    a_pct = np.clip(np.histogram(a, edges)[0] / len(a), PSI_FLOOR, None)
    return float(np.sum((a_pct - e_pct) * np.log(a_pct / e_pct)))


def load_history() -> pd.DataFrame:
    cols = ["zone_id", "hour_ts", "trips", *DEMAND_FEATURES, *SEASONAL_FEATURES]
    df = pd.read_parquet(TRAINING, columns=cols)
    ts = pd.to_datetime(df["hour_ts"])
    df["hour_ts"] = ts.dt.tz_localize(None) if ts.dt.tz is not None else ts
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(float)
    return df


def main() -> int:
    mlflow.set_tracking_uri(TRACKING_URI)
    mv = MlflowClient().get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
    model = mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}@{MODEL_ALIAS}")
    features = mv.tags["features"].split(",")
    reference_wape = float(mv.tags["test_WAPE_pct"])
    threshold = reference_wape * ACCURACY_TOLERANCE

    store = load_online_store(ONLINE_STORE)
    history = load_history()

    # ---------- 1. Accuracy: forecasts (from the online store) vs actuals ----------
    served = store.merge(
        history[["zone_id", "hour_ts", "trips"]], on=["zone_id", "hour_ts"]
    )
    served["pred"] = np.clip(
        model.predict(build_matrix(served, features, model)), 0, None
    )
    served["abs_err"] = (served["pred"] - served["trips"]).abs()
    served["abs_err_base"] = (served["same_hour_4wk_avg"] - served["trips"]).abs()
    served["date"] = served["hour_ts"].dt.date

    daily = served.groupby("date")[["trips", "abs_err", "abs_err_base"]].sum()
    daily["model_wape"] = daily["abs_err"] / daily["trips"] * 100
    daily["baseline_wape"] = daily["abs_err_base"] / daily["trips"] * 100
    daily["rolling_3d_wape"] = (
        daily["abs_err"].rolling(3).sum() / daily["trips"].rolling(3).sum() * 100
    )
    daily["alert"] = daily["rolling_3d_wape"] > threshold

    print(
        f"=== 1. Daily accuracy: @{MODEL_ALIAS} = v{mv.version} ({mv.tags.get('feature_set', '')}) ==="
    )
    print(
        f"Reference WAPE at training: {reference_wape:.2f}% | alert above {threshold:.2f}% (3-day rolling)\n"
    )
    report = daily[
        ["trips", "model_wape", "baseline_wape", "rolling_3d_wape", "alert"]
    ].copy()
    report["trips"] = report["trips"].astype(int)
    print(report.round(2).to_string())
    beat = (daily["model_wape"] < daily["baseline_wape"]).sum()
    print(f"\nModel beat the 4-week baseline on {beat} of {len(daily)} days")

    # ---------- 2. Drift: training period vs serving window ----------
    reference = history[history["hour_ts"] < TRAIN_END]
    rows = []
    for f in DEMAND_FEATURES + SEASONAL_FEATURES:
        value = psi(reference[f], store[f])
        alerting = f in DEMAND_FEATURES
        if np.isnan(value):
            status = "no data"
        elif value <= 0.1:
            status = "stable"
        elif value <= PSI_ALERT:
            status = "moderate"
        else:
            status = "MAJOR SHIFT" if alerting else "shift (seasonal, expected)"
        rows.append(
            {
                "feature": f,
                "type": "demand" if alerting else "seasonal",
                "psi": value,
                "status": status,
                "alert": alerting and value > PSI_ALERT,
            }
        )
    drift = pd.DataFrame(rows)
    print("\n=== 2. Feature drift (PSI, training period vs serving window) ===")
    print(drift.round(3).to_string(index=False))

    # ---------- 3. Completeness: missing rate per model feature ----------
    numeric_features = [f for f in features if f not in ("zone_id", "borough")]
    completeness = pd.DataFrame(
        {
            "feature": numeric_features,
            "missing_serving": [store[f].isna().mean() for f in numeric_features],
            "missing_training": [
                reference[f].isna().mean() if f in reference.columns else np.nan
                for f in numeric_features
            ],
        }
    )
    completeness["alert"] = completeness["missing_serving"] > NULL_ALERT
    gaps = completeness[
        (completeness["missing_serving"] > 0) | (completeness["missing_training"] > 0)
    ]
    print(
        f"\n=== 3. Completeness (alert if > {NULL_ALERT:.0%} missing in serving window) ==="
    )
    if gaps.empty:
        print("All model features complete in both periods")
    else:
        print(gaps.round(3).to_string(index=False))
    print(
        "Note: lag features are legitimately missing at the very start of history; "
        "training-period gaps are a data-coverage finding, not a serving alert."
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    daily.round(4).to_csv(OUT_DIR / "daily_accuracy.csv")
    drift.round(4).to_csv(OUT_DIR / "feature_drift.csv", index=False)
    completeness.round(4).to_csv(OUT_DIR / "feature_completeness.csv", index=False)

    accuracy_alert = bool(daily["alert"].any())
    drift_alert = bool(drift["alert"].any())
    completeness_alert = bool(completeness["alert"].any())
    print("\n=== Verdict ===")
    print(
        f"Accuracy alert: {'YES' if accuracy_alert else 'no'} | "
        f"Drift alert: {'YES' if drift_alert else 'no'} | "
        f"Completeness alert: {'YES' if completeness_alert else 'no'}"
    )
    print(f"Reports written to {OUT_DIR}")
    return 1 if (accuracy_alert or drift_alert or completeness_alert) else 0


if __name__ == "__main__":
    sys.exit(main())
