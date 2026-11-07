"""Phase 7.2 - Baselines for day-ahead demand forecasting.

Three simple rules a planner could use without ML. The model in 7.3 must beat them.

    python forecasting/baselines.py            # uses cached features
    python forecasting/baselines.py --refresh  # re-download from Databricks
"""

from __future__ import annotations

import argparse

from common import load_features, metrics, split

BASELINES = {
    "Same hour yesterday (lag 24h)": "lag_24h",
    "Same hour last week (lag 168h)": "lag_168h",
    "Same hour, 4-week average": "same_hour_4wk_avg",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Forecasting baselines")
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    df = load_features(args.refresh)
    train, valid, test = split(df)
    print(
        f"Rows: train {len(train):,} | validation {len(valid):,} | test {len(test):,}"
    )

    for split_name, data in (
        ("VALIDATION (Mar-Apr 2025)", valid),
        ("TEST (May-Jun 2025)", test),
    ):
        print(f"\n=== Baselines on {split_name} ===")
        print(f"{'Method':<34} {'MAE':>7} {'RMSE':>7} {'WAPE %':>8} {'Bias %':>8}")
        for name, column in BASELINES.items():
            m = metrics(data["trips"], data[column])
            print(
                f"{name:<34} {m['MAE']:7.2f} {m['RMSE']:7.2f} {m['WAPE_pct']:8.2f} {m['Bias_pct']:+8.2f}"
            )


if __name__ == "__main__":
    main()
