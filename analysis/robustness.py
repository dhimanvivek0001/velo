"""Phase 8.5 - Robustness checks for the congestion pricing estimate.

B. Placebo dates: pretend the policy started on a date when nothing happened -> expect ~0.
C. Donut: drop the boundary zones (partly inside the charging zone) from the comparison group.

    python analysis/robustness.py
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from databricks.sdk.core import Config
from did_congestion import HAC, PROJECT_ROOT, build_ratio, load_data

from databricks import sql

CACHE_DIR = PROJECT_ROOT / "data" / "analysis"

THREE_WAY_SQL = """
select f.date_day,
       case when c.is_cbd then 'CBD'
            when c.fee_share_post >= 0.5 then 'Boundary'
            else 'Rest' end as area,
       sum(f.trips) as trips
from velo.gold.fct_zone_hour f
join velo.gold.cbd_zones c on f.zone_id = c.zone_id
where f.borough not in ('EWR', 'N/A', 'Unknown')
group by 1, 2
"""

BOUNDARY_SQL = """
select zone_name, borough, fee_share_post
from velo.gold.cbd_zones
where not is_cbd and fee_share_post >= 0.5
order by fee_share_post desc
"""


def cached_query(name: str, query: str) -> pd.DataFrame:
    """Run a query once against the SQL warehouse and cache the result as CSV."""
    path = CACHE_DIR / f"{name}.csv"
    if path.exists():
        return pd.read_csv(path)
    cfg = Config(profile=os.environ.get("DATABRICKS_PROFILE", "velo"))
    with (
        sql.connect(
            server_hostname=cfg.host.replace("https://", ""),
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            credentials_provider=lambda: cfg.authenticate,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(query)
        df = cur.fetchall_arrow().to_pandas()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return df


def effect(formula: str, data: pd.DataFrame, term: str) -> str:
    model = smf.ols(formula, data=data).fit(**HAC)
    coef = model.params[term]
    low, high = model.conf_int().loc[term]
    pct, pct_low, pct_high = ((np.exp([coef, low, high]) - 1) * 100).tolist()
    return (
        f"{pct:+.2f}%  (95% CI {pct_low:+.2f} to {pct_high:+.2f}, "
        f"p = {model.pvalues[term]:.4f}, n = {int(model.nobs)} days)"
    )


def main() -> None:
    wide = build_ratio(load_data(refresh=False))
    clean = wide[~wide["in_holiday_window"] & ~wide["is_holiday"]].copy()

    print("\n=== 8.5B Placebo tests: fake policy dates, pre-policy data only ===")
    placebos = [
        ("2024-10-06", "2024-07-01", "2024-12-21"),
        ("2024-04-07", "2024-01-01", "2024-06-30"),
    ]
    for fake, start, end in placebos:
        data = clean[
            clean["date_day"].between(pd.Timestamp(start), pd.Timestamp(end))
        ].copy()
        data["fake_post"] = (data["date_day"] >= pd.Timestamp(fake)).astype(int)
        print(
            f"Fake start {fake} (window {start} to {end}): "
            + effect("log_ratio ~ fake_post + C(day_name)", data, "fake_post")
        )

    print("\n=== 8.5C Donut: boundary zones removed from the comparison group ===")
    boundary = cached_query("boundary_zones", BOUNDARY_SQL)
    print(f"Boundary zones ({len(boundary)}): " + ", ".join(boundary["zone_name"]))

    three = cached_query("daily_area_trips_3way", THREE_WAY_SQL)
    three["date_day"] = pd.to_datetime(three["date_day"])
    rest_only = three[three["area"] == "Rest"][["date_day", "trips"]].rename(
        columns={"trips": "rest_only"}
    )
    donut = clean.merge(rest_only, on="date_day")
    donut["log_ratio_donut"] = np.log(donut["CBD"]) - np.log(donut["rest_only"])
    first_half = donut[donut["month_of_year"] <= 6]

    formula = "~ post + C(month_of_year) + C(day_name)"
    print(
        "Year-over-year, original control: "
        + effect(f"log_ratio {formula}", first_half, "post")
    )
    print(
        "Year-over-year, donut control:    "
        + effect(f"log_ratio_donut {formula}", first_half, "post")
    )


if __name__ == "__main__":
    main()
