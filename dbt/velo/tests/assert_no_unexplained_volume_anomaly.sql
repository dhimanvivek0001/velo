-- WARN when a day's volume is abnormal and nothing explains it
-- (not a holiday, not in the known_events calendar).
{{ config(severity='warn') }}

select date_day, day_name, trips, baseline_trips, pct_vs_baseline, z_score
from {{ ref('dq_daily_volume') }}
where is_unexplained_anomaly
