-- Training set assembled from the FEATURE STORE with point-in-time joins.
-- prediction_ts = when the forecast is made (24h before the target hour).
-- A feature row is only joined if available_at <= prediction_ts, so future data cannot leak in.
-- Shared columns keep the legacy names, so Phase 7 code works unchanged.

with labels as (
    select
        zone_id,
        hour_ts,
        hour_ts - interval 24 hours as prediction_ts,
        trips
    from {{ ref('zone_hour_observations') }}
    where hour_ts >= timestamp'2024-01-29 00:00:00'   -- first 4 weeks only provide lag history
)

select
    -- keys
    l.zone_id,
    l.hour_ts,
    l.prediction_ts,
    to_date(l.hour_ts)            as date_day,
    -- zone entity
    z.borough,
    z.is_manhattan_core,
    z.is_airport,
    z.profile_avg_hourly_trips,
    z.profile_night_share,
    z.profile_zero_hour_share,
    z.profile_weekend_lift,
    -- calendar entity
    hour(l.hour_ts)               as hour_of_day,
    dayofweek(l.hour_ts)          as day_of_week,
    c.is_holiday,
    c.is_post_cbd_pricing,
    c.is_long_weekend,
    c.days_to_next_holiday,
    c.days_since_last_holiday,
    c.is_day_before_holiday,
    c.is_day_after_holiday,
    -- weather entity
    w.temp_c,
    w.precip_mm,
    w.is_wet,
    w.is_freezing,
    -- zone x hour demand entity
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
    -- label
    l.trips
from labels l
join {{ ref('fs_zone_hour_demand') }} d
    on d.zone_id = l.zone_id
   and d.hour_ts = l.hour_ts
   and d.available_at <= l.prediction_ts
join {{ ref('fs_zone') }} z
    on z.zone_id = l.zone_id
join {{ ref('fs_calendar') }} c
    on c.date_day = to_date(l.hour_ts)
left join {{ ref('fs_weather_hour') }} w
    on w.weather_hour = l.hour_ts
   and w.available_at <= l.prediction_ts
