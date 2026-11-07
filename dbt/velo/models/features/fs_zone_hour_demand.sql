{{ config(post_hook=[
    "alter table {{ this }} drop constraint if exists fs_zone_hour_demand_pk",
    "alter table {{ this }} alter column zone_id set not null",
    "alter table {{ this }} alter column hour_ts set not null",
    "alter table {{ this }} add constraint fs_zone_hour_demand_pk primary key (zone_id, hour_ts)"
]) }}

-- Feature table: ZONE x HOUR entity, for a 24-hour-ahead forecast.
-- Row = the hour being forecast (hour_ts).
-- available_at = the moment every value in the row was known = hour_ts - 24h.
-- Every window ends at or before available_at (24 rows back), so nothing newer can leak in.

with obs as (
    select zone_id, hour_ts, trips
    from {{ ref('zone_hour_observations') }}
),

lagged as (
    select
        zone_id,
        hour_ts,
        lag(trips, 24)  over w                                         as lag_24h,
        lag(trips, 48)  over w                                         as lag_48h,
        lag(trips, 168) over w                                         as lag_168h,
        lag(trips, 336) over w                                         as lag_336h,
        cast((lag(trips, 168) over w + lag(trips, 336) over w
              + lag(trips, 504) over w + lag(trips, 672) over w) / 4.0 as double)
                                                                       as same_hour_4wk_avg,
        cast(avg(trips) over (partition by zone_id order by hour_ts
                              rows between 191 preceding and 24 preceding) as double)
                                                                       as avg_prev_7d,
        cast(avg(trips) over (partition by zone_id order by hour_ts
                              rows between 695 preceding and 24 preceding) as double)
                                                                       as avg_prev_28d,
        sum(trips) over (partition by zone_id order by hour_ts
                         rows between 47 preceding and 24 preceding)   as sum_prev_24h
    from obs
    window w as (partition by zone_id order by hour_ts)
)

select
    zone_id,
    hour_ts,
    hour_ts - interval 24 hours                                        as available_at,
    lag_24h,
    lag_48h,
    lag_168h,
    lag_336h,
    same_hour_4wk_avg,
    avg_prev_7d,
    avg_prev_28d,
    sum_prev_24h,
    avg_prev_7d / nullif(avg_prev_28d, 0)                              as trend_7d_vs_28d,
    cast((lag_24h + 1.0) / (same_hour_4wk_avg + 1.0) as double)        as yday_vs_4wk_ratio,
    current_timestamp()                                                as computed_at
from lagged
