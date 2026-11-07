"""Phase 6.4: retrain the day-ahead forecast on the FEATURE STORE training set.

Ablation: each feature group is added to the Phase 7 feature set (v0) ON ITS OWN, plus one
run with all groups, so each group's contribution is measured cleanly.
Model choice on VALIDATION. All runs are scored on TEST for reporting only.

Lesson from the first run: raw holiday distances (days_to_next, days_since_last) act as a
DATE FINGERPRINT (holidays are irregularly spaced, so the pair is near-unique per day) and
let the model memorise individual training days. The fix lives in the feature store:
fs_calendar provides days_to_holiday_capped / days_since_holiday_capped (capped at 7 days),
and the raw columns are marked ANALYSIS ONLY in the registry.
"""

from __future__ import annotations

import time

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from common import PROJECT_ROOT, metrics, split
from train_lgbm import BASE_PARAMS, CATEGORICAL
from train_lgbm import FEATURES as LEGACY_FEATURES
from train_lgbm import FLAGS as LEGACY_FLAGS

TABLE = "velo.gold.ml_training_set"
DATA_FILE = PROJECT_ROOT / "data" / "ml" / "ml_training_set.parquet"
HTTP_PATH = "/sql/1.0/warehouses/5cf4b93778132e09"
HOLIDAY_WINDOW_DAYS = 7  # must match the cap in fs_calendar.sql

GROUPS = {
    "calendar": [
        "is_long_weekend",
        "is_day_before_holiday",
        "is_day_after_holiday",
        "days_to_holiday_capped",
        "days_since_holiday_capped",
    ],
    "demand_trend": [
        "avg_prev_28d",
        "sum_prev_24h",
        "trend_7d_vs_28d",
        "yday_vs_4wk_ratio",
    ],
    "zone_profile": [
        "profile_avg_hourly_trips",
        "profile_night_share",
        "profile_zero_hour_share",
        "profile_weekend_lift",
    ],
}
NEW_FLAGS = ["is_long_weekend", "is_day_before_holiday", "is_day_after_holiday"]
PARAMS = {**BASE_PARAMS, "objective": "poisson"}  # Phase 7 winner


def feature_sets() -> dict[str, list[str]]:
    """v0, v0 + each group on its own, v0 + all groups."""
    sets = {"fs_v0_legacy": list(LEGACY_FEATURES)}
    for group, cols in GROUPS.items():
        sets[f"fs_v0_plus_{group}"] = list(LEGACY_FEATURES) + cols
    sets["fs_v0_plus_all"] = list(LEGACY_FEATURES) + [
        c for cols in GROUPS.values() for c in cols
    ]
    return sets


def load_training_set() -> pd.DataFrame:
    """Download the feature-store training set once, then read the local parquet cache."""
    if not DATA_FILE.exists():
        from databricks.sdk.core import Config

        from databricks import sql

        cfg = Config(profile="velo")
        print(f"Downloading {TABLE} (first run only, a few minutes) ...")
        with (
            sql.connect(
                server_hostname=cfg.host.replace("https://", ""),
                http_path=HTTP_PATH,
                credentials_provider=lambda: cfg.authenticate,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(f"select * from {TABLE}")
            table = cur.fetchall_arrow()
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, DATA_FILE)

    df = pd.read_parquet(DATA_FILE)
    for col in ("hour_ts", "prediction_ts"):
        ts = pd.to_datetime(df[col])
        if ts.dt.tz is not None:
            ts = ts.dt.tz_localize(None)
        df[col] = ts
    # Databricks DECIMAL columns and nullable flags arrive as Python objects: make them float
    for col in df.columns.drop(["borough", "date_day", "hour_ts", "prediction_ts"]):
        if df[col].dtype == object:
            df[col] = df[col].astype(float)
    df = df.sort_values(["hour_ts", "zone_id"]).reset_index(drop=True)

    missing = [c for cols in GROUPS.values() for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(
            f"Columns missing from {TABLE}: {missing}\n"
            f"Rebuild dbt (fs_calendar+) and delete {DATA_FILE} so it re-downloads."
        )
    print(f"Loaded {len(df):,} rows x {df.shape[1]} columns from {TABLE}")
    return df


def fingerprint_check(train: pd.DataFrame) -> None:
    """How close do the holiday-distance features come to identifying the exact date?"""
    days = train[
        [
            "date_day",
            "days_to_next_holiday",
            "days_since_last_holiday",
            "days_to_holiday_capped",
            "days_since_holiday_capped",
        ]
    ].drop_duplicates("date_day")
    n_dates = len(days)
    raw = len(
        days[["days_to_next_holiday", "days_since_last_holiday"]].drop_duplicates()
    )
    capped = len(
        days[["days_to_holiday_capped", "days_since_holiday_capped"]].drop_duplicates()
    )
    print("\n=== Date-fingerprint check (training period) ===")
    print(f"Distinct training dates                    : {n_dates}")
    print(
        f"Distinct (days_to, days_since) pairs, raw  : {raw}  ({raw / n_dates:.0%} of dates)"
    )
    print(
        f"Distinct pairs, capped at {HOLIDAY_WINDOW_DAYS} days         : "
        f"{capped}  ({capped / n_dates:.0%} of dates)"
    )


def prepare(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    X = df[features].copy()
    for col in set(LEGACY_FLAGS + NEW_FLAGS) & set(features):
        X[col] = X[col].astype(float)
    for col in CATEGORICAL:
        X[col] = pd.Categorical(X[col], categories=sorted(df[col].dropna().unique()))
    return X


def segment_table(test: pd.DataFrame, preds: dict[str, pd.Series]) -> pd.DataFrame:
    """Test WAPE by segment: the places Phase 7 said the model was weakest."""
    segments = {
        "All hours": pd.Series(True, index=test.index),
        "Long-weekend days": test["is_long_weekend"] == 1,
        "Public holidays": test["is_holiday"] == 1,
        "Day before a holiday": test["is_day_before_holiday"] == 1,
        "Airports (JFK, LGA)": test["is_airport"] == 1,
        "Quiet zones (<10/h)": test["profile_avg_hourly_trips"] < 10,
        "Night (00-05)": test["hour_of_day"] < 6,
    }
    rows = []
    for name, mask in segments.items():
        sub = test[mask]
        if sub.empty:
            continue
        row = {
            "segment": name,
            "n": len(sub),
            "base": metrics(sub["trips"], sub["same_hour_4wk_avg"])["WAPE_pct"],
        }
        for run_name, pred in preds.items():
            short = run_name.replace("fs_v0_plus_", "+").replace("fs_v0_legacy", "v0")
            row[short] = metrics(sub["trips"], pred[mask])["WAPE_pct"]
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    df = load_training_set()
    y = df["trips"].astype(float)
    train, valid, test = split(df)
    fingerprint_check(train)

    mlflow.set_tracking_uri(f"sqlite:///{(PROJECT_ROOT / 'mlflow.db').as_posix()}")
    mlflow.set_experiment("velo-day-ahead-demand")

    results = {}
    for run_name, features in feature_sets().items():
        X = prepare(df, features)
        X_tr, y_tr = X.loc[train.index], y.loc[train.index]
        X_va, y_va = X.loc[valid.index], y.loc[valid.index]
        print(f"\nTraining {run_name} ({len(features)} features) ...")
        start = time.time()
        with mlflow.start_run(run_name=run_name) as run:
            mlflow.set_tags(
                {
                    "feature_set": run_name,
                    "source": "feature_store",
                    "training_table": TABLE,
                    "ablation": "independent_v3_store_capped_calendar",
                }
            )
            dtrain = lgb.Dataset(X_tr, y_tr, categorical_feature=CATEGORICAL)
            dvalid = lgb.Dataset(X_va, y_va, reference=dtrain)
            model = lgb.train(
                PARAMS,
                dtrain,
                num_boost_round=1500,
                valid_sets=[dvalid],
                callbacks=[
                    lgb.early_stopping(50, verbose=False),
                    lgb.log_evaluation(250),
                ],
            )
            pred = np.clip(
                model.predict(X_va, num_iteration=model.best_iteration), 0, None
            )
            m = metrics(y_va, pred)
            mlflow.log_params(PARAMS)
            mlflow.log_params(
                {
                    "n_features": len(features),
                    "best_iteration": model.best_iteration,
                    "features": ",".join(features),
                    "holiday_window_days": HOLIDAY_WINDOW_DAYS,
                }
            )
            mlflow.log_metrics({f"valid_{k}": v for k, v in m.items()})
            mlflow.lightgbm.log_model(model, name="model")
            results[run_name] = {
                "valid": m,
                "model": model,
                "run_id": run.info.run_id,
                "features": features,
            }
        print(
            f"{run_name}: valid WAPE {m['WAPE_pct']:.2f}% | bias {m['Bias_pct']:+.2f}% | "
            f"trees {model.best_iteration} | {time.time() - start:.0f}s"
        )

    # Choose on VALIDATION only
    best_name = min(results, key=lambda k: results[k]["valid"]["WAPE_pct"])

    # Score every run on TEST for the report (selection above did not look at these)
    test_preds, test_metrics = {}, {}
    for run_name, r in results.items():
        X_te = prepare(df, r["features"]).loc[test.index]
        pred = np.clip(
            r["model"].predict(X_te, num_iteration=r["model"].best_iteration), 0, None
        )
        test_preds[run_name] = pd.Series(pred, index=test.index)
        test_metrics[run_name] = metrics(y.loc[test.index], pred)
        with mlflow.start_run(run_id=r["run_id"]):
            mlflow.log_metrics(
                {f"test_{k}": v for k, v in test_metrics[run_name].items()}
            )
    base_test = metrics(y.loc[test.index], test["same_hour_4wk_avg"])

    print("\n=== Ablation (WAPE %, lower is better) ===")
    print(f"{'Run':<26}{'Features':>9}{'Trees':>7}{'Validation':>12}{'Test*':>8}")
    print(
        f"{'Baseline: 4-week average':<26}{'-':>9}{'-':>7}{'':>12}{base_test['WAPE_pct']:>8.2f}"
    )
    for run_name, r in results.items():
        print(
            f"{run_name:<26}{len(r['features']):>9}{r['model'].best_iteration:>7}"
            f"{r['valid']['WAPE_pct']:>12.2f}{test_metrics[run_name]['WAPE_pct']:>8.2f}"
        )
    print("* test shown for reporting; the winner is chosen on validation only")

    v0, best = (
        test_metrics["fs_v0_legacy"]["WAPE_pct"],
        test_metrics[best_name]["WAPE_pct"],
    )
    print(f"\nWinner on validation: {best_name}")
    print(
        f"Test WAPE v0 {v0:.2f}% -> winner {best:.2f}%  ({(v0 - best) / v0 * 100:+.1f}% relative)"
    )

    seg = segment_table(test, test_preds)
    print("\n=== Test WAPE by segment (%) ===")
    print(seg.round(2).to_string(index=False))
    with mlflow.start_run(run_id=results[best_name]["run_id"]):
        mlflow.log_text(seg.round(3).to_csv(index=False), "segment_wape.csv")

    full = results["fs_v0_plus_all"]
    gain = pd.Series(full["model"].feature_importance("gain"), index=full["features"])
    share = gain / gain.sum() * 100
    new_cols = [c for cols in GROUPS.values() for c in cols]
    print("\nNew features in fs_v0_plus_all (share of total gain):")
    for feat, pct in share[new_cols].sort_values(ascending=False).items():
        print(f"  {feat:<26} {pct:5.1f}%")


if __name__ == "__main__":
    main()
