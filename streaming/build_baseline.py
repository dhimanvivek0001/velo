"""Export 'normal' demand per zone / day-of-week / hour from fct_zone_hour.

Run once (or after the gold layer is rebuilt):
    python streaming/build_baseline.py
"""

from __future__ import annotations

import os
from pathlib import Path

from databricks.sdk.core import Config
from dotenv import load_dotenv

from databricks import sql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
OUT_FILE = PROJECT_ROOT / "data" / "stream" / "baseline_zone_hour.csv"

# Average trips per hour = total trips / number of such days.
# Dividing by the number of days (not rows) counts zero-trip hours correctly.
QUERY = """
with days as (
    select dayofweek(date_day) as dow, count(*) as n_days
    from velo.gold.dim_date
    where not is_holiday
    group by dayofweek(date_day)
)
select
    f.zone_id,
    dayofweek(f.date_day)           as dow,
    f.hour_of_day,
    sum(f.trips) / max(d.n_days)    as avg_trips_per_hour
from velo.gold.fct_zone_hour f
join days d on dayofweek(f.date_day) = d.dow
where not f.is_holiday
group by f.zone_id, dayofweek(f.date_day), f.hour_of_day
"""


def main() -> None:
    cfg = Config(profile=os.environ.get("DATABRICKS_PROFILE", "velo"))
    with (
        sql.connect(
            server_hostname=cfg.host.replace("https://", ""),
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            credentials_provider=lambda: cfg.authenticate,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(QUERY)
        df = cur.fetchall_arrow().to_pandas()

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_FILE, index=False)
    print(f"Saved {len(df):,} baseline rows to {OUT_FILE}")


if __name__ == "__main__":
    main()
