"""Phase 7.5 / 7.6 - What drives demand (SHAP) and what the better forecast is worth.

SHAP values come from LightGBM itself (pred_contrib=True): for each prediction, how much
each feature pushed it up or down. For the Poisson model these are on the log scale, so
exp(value) - 1 reads as a % effect on predicted demand.

    python forecasting/explain.py
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import mlflow.lightgbm
import numpy as np
import pandas as pd
from common import PROJECT_ROOT, load_features, split
from evaluate import best_run_id
from train_lgbm import FEATURES, prepare

IMG_DIR = PROJECT_ROOT / "docs" / "images"
TRIPS_PER_DRIVER_HOUR = (
    2.0  # ASSUMPTION for 7.6 (illustrative; change to test sensitivity)
)


def main() -> None:
    df = load_features()
    X = prepare(df)
    _train, _valid, test = split(df)
    test = test.copy()
    model = mlflow.lightgbm.load_model(f"runs:/{best_run_id()}/model")

    # ---------- 7.5 SHAP: what drives the forecast ----------
    sample_idx = test.sample(50_000, random_state=42).index
    contrib = model.predict(X.loc[sample_idx], pred_contrib=True)
    shap = pd.DataFrame(
        contrib[:, :-1], columns=FEATURES, index=sample_idx
    )  # last column = base value

    importance = shap.abs().mean()
    share = (importance / importance.sum() * 100).sort_values(ascending=False)
    print("\n=== 7.5 What drives the forecast (mean |SHAP|, share of total) ===")
    for feat, pct in share.head(10).items():
        print(f"  {feat:<22} {pct:5.1f}%")

    print("\n=== Average effect on predicted demand, all else equal ===")
    for flag, label in (
        ("is_holiday", "Public holiday"),
        ("is_wet", "Rain or snow"),
        ("is_freezing", "Freezing temperature"),
    ):
        on = X.loc[sample_idx, flag] == 1
        if on.sum() > 0:
            effect = (np.exp(shap.loc[on, flag].mean()) - 1) * 100
            print(
                f"  {label:<22} {effect:+5.1f}%   ({on.sum():,} zone-hours in sample)"
            )

    ax = share.head(10).sort_values().plot.barh(figsize=(8, 5), color="tab:blue")
    ax.set_xlabel("Share of total SHAP importance (%)")
    ax.set_title("What drives the day-ahead demand forecast")
    plt.tight_layout()
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    plt.savefig(IMG_DIR / "forecast_shap_importance.png", dpi=150)
    plt.close()

    # ---------- 7.6 Business value ----------
    test["pred"] = np.clip(model.predict(X.loc[test.index]), 0, None)
    days = test["date_day"].nunique()
    base_err = (test["same_hour_4wk_avg"] - test["trips"]).abs().sum() / days
    model_err = (test["pred"] - test["trips"]).abs().sum() / days
    saved = base_err - model_err

    print("\n=== 7.6 Business value (test period, May-Jun 2025) ===")
    print(
        f"Forecast error per day, baseline : {base_err:>10,.0f} trips (sum of |error| over all zone-hours)"
    )
    print(f"Forecast error per day, LightGBM : {model_err:>10,.0f} trips")
    print(
        f"=> {saved:,.0f} fewer trips mis-forecast per day ({saved / base_err * 100:.1f}% less)"
    )
    print(
        f"\nASSUMPTION: one driver completes ~{TRIPS_PER_DRIVER_HOUR:g} trips per hour"
    )
    print(
        f"=> ~{saved / TRIPS_PER_DRIVER_HOUR:,.0f} driver-hours per day better placed "
        f"(~{saved / TRIPS_PER_DRIVER_HOUR * 365:,.0f} per year)"
    )
    print(
        "   i.e. fewer idle drivers where demand was over-forecast, fewer unserved riders where under-forecast"
    )
    print(f"\nChart saved: {IMG_DIR / 'forecast_shap_importance.png'}")


if __name__ == "__main__":
    main()
