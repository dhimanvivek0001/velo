"""Phase 8 - Causal analysis: did congestion pricing reduce ride-hail demand in the CBD?

8.2 parallel trends, 8.3 difference-in-differences, 8.5A year-over-year control.
Data: Jan 2024 - Jun 2025 (Jan-Jun 2024 gives a same-season comparison without the fee).

    python analysis/did_congestion.py            # uses cached data if present
    python analysis/did_congestion.py --refresh  # re-pull from Databricks
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from databricks.sdk.core import Config
from dotenv import load_dotenv

from databricks import sql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
DATA_FILE = PROJECT_ROOT / "data" / "analysis" / "daily_area_trips.csv"
IMG_DIR = PROJECT_ROOT / "docs" / "images"
POLICY_DATE = pd.Timestamp("2025-01-05")
HOLIDAY_WINDOW = (pd.Timestamp("2024-12-22"), pd.Timestamp("2025-01-04"))
HAC = {
    "cov_type": "HAC",
    "cov_kwds": {"maxlags": 7},
}  # robust to day-to-day autocorrelation


def load_data(refresh: bool) -> pd.DataFrame:
    """Daily trips per area from the dbt model (cached locally as CSV)."""
    if DATA_FILE.exists() and not refresh:
        return pd.read_csv(DATA_FILE, parse_dates=["date_day"])

    cfg = Config(profile=os.environ.get("DATABRICKS_PROFILE", "velo"))
    with (
        sql.connect(
            server_hostname=cfg.host.replace("https://", ""),
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            credentials_provider=lambda: cfg.authenticate,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute("select * from velo.gold.daily_area_trips order by date_day, area")
        df = cur.fetchall_arrow().to_pandas()

    df["date_day"] = pd.to_datetime(df["date_day"])
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DATA_FILE, index=False)
    return df


def build_ratio(df: pd.DataFrame) -> pd.DataFrame:
    """One row per day: CBD trips, rest trips, and log(CBD) - log(rest)."""
    wide = df.pivot_table(
        index="date_day", columns="area", values="trips"
    ).reset_index()
    meta = df.drop_duplicates("date_day").set_index("date_day")[
        ["day_name", "is_holiday", "is_post"]
    ]
    wide = wide.join(meta, on="date_day")
    wide["log_ratio"] = np.log(wide["CBD"]) - np.log(wide["Rest of NYC"])
    wide["days_from_policy"] = (wide["date_day"] - POLICY_DATE).dt.days
    wide["week"] = np.floor(wide["days_from_policy"] / 7).astype(int)
    wide["year"] = wide["date_day"].dt.year
    wide["month_of_year"] = wide["date_day"].dt.month
    wide["in_holiday_window"] = wide["date_day"].between(*HOLIDAY_WINDOW)
    wide["is_holiday"] = wide["is_holiday"].astype(bool)
    wide["post"] = wide["is_post"].astype(bool).astype(int)
    return wide


def parallel_trends(wide: pd.DataFrame) -> None:
    """8.2 - Test for drift in the 6 months before the policy; plot all weeks."""
    pre = wide[
        wide["days_from_policy"].between(-187, -1) & ~wide["in_holiday_window"]
    ].copy()
    pre_mean = pre["log_ratio"].mean()

    model = smf.ols("log_ratio ~ days_from_policy + C(day_name)", data=pre).fit(**HAC)
    slope_30d = model.params["days_from_policy"] * 30 * 100
    ci_low, ci_high = (model.conf_int().loc["days_from_policy"] * 30 * 100).tolist()

    print("\n=== 8.2 Parallel trends check (Jul-Dec 2024, holidays excluded) ===")
    print(
        f"Pre-period drift in CBD/rest ratio: {slope_30d:+.2f}% per 30 days "
        f"(95% CI {ci_low:+.2f} to {ci_high:+.2f}, p = {model.pvalues['days_from_policy']:.3f})"
    )

    weekly = (wide.groupby("week")["log_ratio"].mean() - pre_mean) * 100
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(
        weekly.index,
        weekly.values,
        marker="o",
        markersize=3,
        label="CBD vs rest of NYC (weekly)",
    )
    ax.axvline(
        -0.5,
        color="red",
        linestyle="--",
        label="Congestion pricing starts (5 Jan 2025)",
    )
    ax.axhline(0, color="grey", linewidth=0.8)
    ax.axvspan(-2.5, -0.5, color="orange", alpha=0.15, label="Christmas-New Year weeks")
    ax.set_xlabel("Weeks relative to 5 Jan 2025")
    ax.set_ylabel("CBD demand relative to rest of NYC\n(% vs Jul-Dec 2024 average)")
    ax.set_title("CBD vs rest-of-NYC ride-hail demand, Jan 2024 - Jun 2025")
    ax.legend(loc="lower left")
    fig.tight_layout()
    fig.savefig(IMG_DIR / "did_parallel_trends.png", dpi=150)
    plt.close(fig)


def did_estimate(wide: pd.DataFrame) -> float:
    """8.3 / 8.5A - DiD estimates from simple to strict; returns the year-over-year coefficient."""
    sample = wide[~wide["in_holiday_window"] & ~wide["is_holiday"]].copy()
    first_half = sample[sample["month_of_year"] <= 6]
    specs = [
        ("A. Simple (post + weekday)", "log_ratio ~ post + C(day_name)", sample),
        (
            "B. Seasonal (+ month-of-year)",
            "log_ratio ~ post + C(month_of_year) + C(day_name)",
            sample,
        ),
        (
            "C. Year-over-year (Jan-Jun 25 vs 24)",
            "log_ratio ~ post + C(month_of_year) + C(day_name)",
            first_half,
        ),
    ]

    print("\n=== 8.3 / 8.5A Difference-in-differences (holidays excluded) ===")
    yoy_coef = 0.0
    for name, formula, data in specs:
        model = smf.ols(formula, data=data).fit(**HAC)
        coef = model.params["post"]
        low, high = model.conf_int().loc["post"]
        pct, pct_low, pct_high = ((np.exp([coef, low, high]) - 1) * 100).tolist()
        print(
            f"{name:<38} effect {pct:+.2f}%  (95% CI {pct_low:+.2f} to {pct_high:+.2f}, "
            f"p = {model.pvalues['post']:.4f}, n = {int(model.nobs)} days)"
        )
        yoy_coef = coef

    post_days = first_half[first_half["post"] == 1]
    actual = post_days["CBD"].mean()
    counterfactual = actual / np.exp(yoy_coef)
    lost_per_day = counterfactual - actual
    print(
        f"\nUsing model C: CBD pickups per day {actual:,.0f} actual vs {counterfactual:,.0f} without the fee"
    )
    print(
        f"=> about {lost_per_day:,.0f} fewer CBD pickups per day, "
        f"~{lost_per_day * len(post_days):,.0f} over {len(post_days)} days"
    )
    return yoy_coef


def monthly_yoy(wide: pd.DataFrame) -> None:
    """Same month, one year apart: CBD share of demand in 2025 vs 2024."""
    sample = wide[
        ~wide["in_holiday_window"] & ~wide["is_holiday"] & (wide["month_of_year"] <= 6)
    ]
    by_month = (
        sample.groupby(["month_of_year", "year"])["log_ratio"].mean().unstack("year")
    )
    by_month["yoy_pct"] = (np.exp(by_month[2025] - by_month[2024]) - 1) * 100

    print("\n=== Month-by-month: CBD share of demand, 2025 vs same month 2024 ===")
    labels = []
    for month, row in by_month.iterrows():
        label = pd.Timestamp(2025, int(month), 1).strftime("%b")
        labels.append(label)
        print(f"{label}: {row['yoy_pct']:+.2f}%")

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(
        labels,
        by_month["yoy_pct"],
        color=["tab:red" if v < 0 else "tab:green" for v in by_month["yoy_pct"]],
    )
    ax.axhline(0, color="grey", linewidth=0.8)
    ax.set_ylabel("CBD share of demand vs same month 2024 (%)")
    ax.set_title(
        "Year-over-year change in CBD share of ride-hail demand (2025 vs 2024)"
    )
    fig.tight_layout()
    fig.savefig(IMG_DIR / "did_yoy_by_month.png", dpi=150)
    plt.close(fig)
    print(f"Charts saved in {IMG_DIR}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Congestion pricing causal analysis")
    parser.add_argument(
        "--refresh", action="store_true", help="re-pull data from Databricks"
    )
    args = parser.parse_args()

    wide = build_ratio(load_data(args.refresh))
    print(
        f"Loaded {len(wide)} days: {wide['date_day'].min():%d %b %Y} - {wide['date_day'].max():%d %b %Y}"
    )
    parallel_trends(wide)
    did_estimate(wide)
    monthly_yoy(wide)


if __name__ == "__main__":
    main()
