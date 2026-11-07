"""Phase 9.2a: materialise the ONLINE feature store for the serving API.

Offline store = Delta feature tables in velo.features (large; used for training).
Online store  = a small snapshot keyed by (zone_id, hour_ts) that the API reads in milliseconds.
It uses the SAME joins as ml_training_set (minus the label), so serving sees exactly the
features the model was trained on. On paid Databricks tiers this would be an Online Table.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_FILE = PROJECT_ROOT / "data" / "online" / "online_features.parquet"
HTTP_PATH = "/sql/1.0/warehouses/5cf4b93778132e09"
DATA_END = "2025-07-01 00:00:00"  # the data ends at 2025-06-30 23:00

QUERY = """
select
    d.zone_id,
    d.hour_ts,
    d.available_at,
    z.borough,
    z.is_manhattan_core,
    z.is_airport,
    z.profile_avg_hourly_trips,
    z.profile_night_share,
    z.profile_zero_hour_share,
    z.profile_weekend_lift,
    hour(d.hour_ts)          as hour_of_day,
    dayofweek(d.hour_ts)     as day_of_week,
    c.is_holiday,
    c.is_post_cbd_pricing,
    c.is_long_weekend,
    c.days_to_holiday_capped,
    c.days_since_holiday_capped,
    c.is_day_before_holiday,
    c.is_day_after_holiday,
    w.temp_c,
    w.precip_mm,
    w.is_wet,
    w.is_freezing,
    d.lag_24h,
    d.lag_48h,
    d.lag_168h,
    d.lag_336h,
    d.same_hour_4wk_avg,
    d.avg_prev_7d,
    d.avg_prev_28d,
    d.sum_prev_24h,
    d.trend_7d_vs_28d,
    d.yday_vs_4wk_ratio,
    current_timestamp()      as materialized_at
from velo.features.fs_zone_hour_demand d
join velo.features.fs_zone z
    on z.zone_id = d.zone_id
join velo.features.fs_calendar c
    on c.date_day = to_date(d.hour_ts)
left join velo.features.fs_weather_hour w
    on w.weather_hour = d.hour_ts
   and w.available_at <= d.available_at
where d.hour_ts >= timestamp'{start}'
  and d.hour_ts <  timestamp'{end}'
"""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync the last N days of features to the online store"
    )
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()

    end = pd.Timestamp(DATA_END)
    start = end - pd.Timedelta(days=args.days)

    from databricks.sdk.core import Config

    from databricks import sql

    cfg = Config(profile="velo")
    print(f"Materialising online store: {start} -> {end} ...")
    with (
        sql.connect(
            server_hostname=cfg.host.replace("https://", ""),
            http_path=HTTP_PATH,
            credentials_provider=lambda: cfg.authenticate,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(QUERY.format(start=start, end=end))
        table = cur.fetchall_arrow()

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, OUT_FILE)
    df = table.to_pandas()
    print(
        f"Online store: {len(df):,} rows | {df['zone_id'].nunique()} zones | "
        f"{df['hour_ts'].min()} -> {df['hour_ts'].max()}"
    )
    print(f"Written {OUT_FILE}")


if __name__ == "__main__":
    main()
