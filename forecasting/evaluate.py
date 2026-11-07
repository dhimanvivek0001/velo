"""Phase 7.4 - Where is the forecast wrong? Error analysis on the TEST period (May-Jun 2025).

Loads the best LightGBM model from MLflow, compares it with the 4-week-average baseline
by hour, borough, zone size and holidays, and lists the biggest misses.

    python forecasting/evaluate.py
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
from common import PROJECT_ROOT, load_features, metrics, split
from train_lgbm import prepare

IMG_DIR = PROJECT_ROOT / "docs" / "images"
ZONES_FILE = PROJECT_ROOT / "dbt" / "velo" / "seeds" / "taxi_zone_lookup.csv"


def best_run_id() -> str:
    mlflow.set_tracking_uri(f"sqlite:///{(PROJECT_ROOT / 'mlflow.db').as_posix()}")
    runs = mlflow.search_runs(
        experiment_names=["velo-day-ahead-demand"],
        filter_string="attributes.status = 'FINISHED' and tags.mlflow.runName LIKE 'lgbm%'",
        order_by=["metrics.valid_WAPE_pct ASC"],
    )
    best = runs.iloc[0]
    print(
        f"Best model from MLflow: {best['tags.mlflow.runName']} "
        f"(valid WAPE {best['metrics.valid_WAPE_pct']:.2f}%)"
    )
    return best["run_id"]


def segment_table(test: pd.DataFrame, by: str, title: str) -> None:
    print(f"\n=== {title} ===")
    print(
        f"{'Segment':<22}{'Trips':>12}{'Baseline WAPE':>15}{'Model WAPE':>12}{'Model bias':>12}"
    )
    for key, g in test.groupby(by, observed=True):
        base = metrics(g["trips"], g["same_hour_4wk_avg"])
        model = metrics(g["trips"], g["pred"])
        print(
            f"{key!s:<22}{g['trips'].sum():>12,.0f}{base['WAPE_pct']:>14.1f}%"
            f"{model['WAPE_pct']:>11.1f}%{model['Bias_pct']:>+11.1f}%"
        )


def main() -> None:
    df = load_features()
    X = prepare(df)
    train, _valid, test = split(df)
    test = test.copy()

    model = mlflow.lightgbm.load_model(f"runs:/{best_run_id()}/model")
    test["pred"] = np.clip(model.predict(X.loc[test.index]), 0, None)

    overall = metrics(test["trips"], test["pred"])
    print(
        f"Test period overall: WAPE {overall['WAPE_pct']:.2f}% | MAE {overall['MAE']:.2f} | "
        f"bias {overall['Bias_pct']:+.2f}%"
    )

    # Segments
    zone_size = train.groupby("zone_id")["trips"].mean()
    test["zone_size"] = pd.cut(
        test["zone_id"].map(zone_size),
        bins=[-1, 10, 50, 150, np.inf],
        labels=[
            "Quiet (<10/h)",
            "Small (10-50/h)",
            "Medium (50-150/h)",
            "Busy (150+/h)",
        ],
    )
    test["day_type"] = np.where(test["is_holiday"] == 1, "Holiday", "Normal day")
    test["hour_band"] = pd.cut(
        test["hour_of_day"],
        bins=[-1, 5, 9, 15, 19, 23],
        labels=[
            "00-05 night",
            "06-09 morning",
            "10-15 midday",
            "16-19 evening",
            "20-23 late",
        ],
    )
    segment_table(test, "hour_band", "By time of day")
    segment_table(test, "borough", "By borough")
    segment_table(test, "zone_size", "By zone size (avg trips per hour)")
    segment_table(test, "day_type", "Holidays vs normal days")

    # Biggest misses: zone-days
    names = pd.read_csv(ZONES_FILE).set_index("LocationID")["Zone"]
    daily = test.groupby(["date_day", "zone_id"], as_index=False)[
        ["trips", "pred"]
    ].sum()
    daily["error"] = daily["pred"] - daily["trips"]
    daily["zone"] = daily["zone_id"].map(names)
    print("\n=== 10 biggest zone-day misses ===")
    worst = daily.reindex(daily["error"].abs().sort_values(ascending=False).index).head(
        10
    )
    for r in worst.itertuples(index=False):
        print(
            f"{r.date_day}  {r.zone:<32} actual {r.trips:>7,.0f}  forecast {r.pred:>7,.0f}  ({r.error:+,.0f})"
        )

    # Charts
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    by_hour = test.groupby("hour_of_day").apply(
        lambda g: pd.Series(
            {
                "Baseline (4-week avg)": metrics(g["trips"], g["same_hour_4wk_avg"])[
                    "WAPE_pct"
                ],
                "LightGBM": metrics(g["trips"], g["pred"])["WAPE_pct"],
            }
        ),
        include_groups=False,
    )
    ax = by_hour.plot(figsize=(10, 4.5), marker="o")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("WAPE (%)")
    ax.set_title("Forecast error by hour of day: test period (May-Jun 2025)")
    plt.tight_layout()
    plt.savefig(IMG_DIR / "forecast_error_by_hour.png", dpi=150)
    plt.close()

    week = test[(test["hour_ts"] >= "2025-06-02") & (test["hour_ts"] < "2025-06-09")]
    city = week.groupby("hour_ts")[["trips", "pred"]].sum()
    ax = city.rename(
        columns={"trips": "Actual", "pred": "Forecast (made a day ahead)"}
    ).plot(figsize=(12, 4.5))
    ax.set_ylabel("Trips per hour (all NYC)")
    ax.set_title("Citywide demand vs day-ahead forecast, 2-8 June 2025")
    plt.tight_layout()
    plt.savefig(IMG_DIR / "forecast_week.png", dpi=150)
    plt.close()
    print(f"\nCharts saved in {IMG_DIR}")


if __name__ == "__main__":
    main()
