"""Phase 7.3 - LightGBM day-ahead demand forecast, tracked in MLflow.

Trains two LightGBM variants (squared-error and Poisson loss) with early stopping
on the validation period, logs everything to MLflow, picks the best on validation,
and scores ONLY that model on the untouched test period.

    python forecasting/train_lgbm.py
    mlflow ui --backend-store-uri sqlite:///mlflow.db     # then open http://localhost:5000
"""

from __future__ import annotations

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
from common import PROJECT_ROOT, load_features, metrics, split

FEATURES = [
    "zone_id",
    "borough",
    "is_manhattan_core",
    "is_airport",
    "hour_of_day",
    "day_of_week",
    "is_holiday",
    "is_post_cbd_pricing",
    "temp_c",
    "precip_mm",
    "is_wet",
    "is_freezing",
    "lag_24h",
    "lag_48h",
    "lag_168h",
    "lag_336h",
    "same_hour_4wk_avg",
    "avg_prev_7d",
]
CATEGORICAL = ["zone_id", "borough"]
FLAGS = [
    "is_manhattan_core",
    "is_airport",
    "is_holiday",
    "is_post_cbd_pricing",
    "is_wet",
    "is_freezing",
]

BASE_PARAMS = {
    "learning_rate": 0.1,
    "num_leaves": 127,
    "min_data_in_leaf": 100,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "metric": "l1",  # early-stop on absolute error (aligned with MAE / WAPE)
    "verbose": -1,
    "seed": 42,
}
VARIANTS = {
    "lgbm_squared_error": {"objective": "regression"},
    "lgbm_poisson": {"objective": "poisson"},
}


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Model matrix: flags as 0/1 (NaN kept), categoricals with one shared category list."""
    X = df[FEATURES].copy()
    for col in FLAGS:
        X[col] = X[col].astype(float)
    for col in CATEGORICAL:
        X[col] = pd.Categorical(X[col], categories=sorted(df[col].dropna().unique()))
    return X


def main() -> None:
    df = load_features()
    X = prepare(df)
    y = df["trips"].astype(float)
    train, valid, test = split(df)
    X_tr, y_tr = X.loc[train.index], y.loc[train.index]
    X_va, y_va = X.loc[valid.index], y.loc[valid.index]
    X_te, y_te = X.loc[test.index], y.loc[test.index]

    mlflow.set_tracking_uri(f"sqlite:///{(PROJECT_ROOT / 'mlflow.db').as_posix()}")
    mlflow.set_experiment("velo-day-ahead-demand")

    # Log the baseline as a run too, so MLflow compares everything side by side
    base_valid = metrics(y_va, valid["same_hour_4wk_avg"])
    with mlflow.start_run(run_name="baseline_4wk_avg"):
        mlflow.log_param("method", "same hour, 4-week average")
        mlflow.log_metrics({f"valid_{k}": v for k, v in base_valid.items()})

    results = {}
    for name, extra in VARIANTS.items():
        params = {**BASE_PARAMS, **extra}
        print(f"\nTraining {name} ...")
        with mlflow.start_run(run_name=name) as run:
            dtrain = lgb.Dataset(X_tr, y_tr, categorical_feature=CATEGORICAL)
            dvalid = lgb.Dataset(X_va, y_va, reference=dtrain)
            model = lgb.train(
                params,
                dtrain,
                num_boost_round=1500,
                valid_sets=[dvalid],
                callbacks=[
                    lgb.early_stopping(50, verbose=False),
                    lgb.log_evaluation(100),
                ],
            )
            pred = np.clip(
                model.predict(X_va, num_iteration=model.best_iteration), 0, None
            )
            m = metrics(y_va, pred)

            mlflow.log_params(params)
            mlflow.log_param("best_iteration", model.best_iteration)
            mlflow.log_metrics({f"valid_{k}": v for k, v in m.items()})
            mlflow.lightgbm.log_model(model, "model")
            results[name] = (m, model, run.info.run_id)
            print(
                f"{name}: valid WAPE {m['WAPE_pct']:.2f}% | MAE {m['MAE']:.2f} | "
                f"bias {m['Bias_pct']:+.2f}% | trees {model.best_iteration}"
            )

    # Pick the best model on VALIDATION, then score it once on TEST
    best_name = min(results, key=lambda k: results[k][0]["WAPE_pct"])
    best_metrics, best_model, best_run = results[best_name]
    test_pred = np.clip(
        best_model.predict(X_te, num_iteration=best_model.best_iteration), 0, None
    )
    test_m = metrics(y_te, test_pred)
    base_test = metrics(y_te, test["same_hour_4wk_avg"])
    with mlflow.start_run(run_id=best_run):
        mlflow.log_metrics({f"test_{k}": v for k, v in test_m.items()})

    print("\n=== Summary (WAPE %, lower is better) ===")
    print(f"{'':<26}{'Validation':>12}{'Test':>10}")
    print(
        f"{'Baseline: 4-week average':<26}{base_valid['WAPE_pct']:>12.2f}{base_test['WAPE_pct']:>10.2f}"
    )
    print(
        f"{'Best: ' + best_name:<26}{best_metrics['WAPE_pct']:>12.2f}{test_m['WAPE_pct']:>10.2f}"
    )
    improvement = (
        (base_test["WAPE_pct"] - test_m["WAPE_pct"]) / base_test["WAPE_pct"] * 100
    )
    print(
        f"\nOn the untouched test period, LightGBM cuts forecast error by {improvement:.1f}% vs the baseline."
    )

    importance = pd.Series(best_model.feature_importance("gain"), index=FEATURES)
    print("\nTop features (share of total gain):")
    for feat, share in (
        (importance / importance.sum() * 100)
        .sort_values(ascending=False)
        .head(8)
        .items()
    ):
        print(f"  {feat:<22} {share:5.1f}%")


if __name__ == "__main__":
    main()
