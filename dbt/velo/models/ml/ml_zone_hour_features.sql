-- Model-ready features for DAY-AHEAD demand forecasting: one row per zone per hour.
-- Complete grid: zero-demand hours are real rows (trips = 0), not missing.
-- Leakage rule: every demand feature is at least 24 hours old.
with zones as (
    select zone_id, borough, is_manhattan_core, is_airport
    from {{ ref('dim_zone') }}
    where borough not in ('EWR', 'N/A', 'Unknown')
),

hours as (
    select explode(sequence(
        timestamp'2024-01-01 00:00:00', timestamp'2025-06-30 23:00:00', interval 1 hour
    )) as hour_ts
),

demand as (
    select z.zone_id, z.borough, z.is_manhattan_core, z.is_airport, h.hour_ts,
           coalesce(f.trips, 0) as trips
    from zones z
    cross join hours h
    left join {{ ref('fct_zone_hour') }} f
        on f.zone_id = z.zone_id and f.hour_ts = h.hour_ts
),

lagged as (
    select
        d.*,
        lag(trips, 24)  over w as lag_24h,      -- same hour yesterday
        lag(trips, 48)  over w as lag_48h,      -- same hour 2 days ago
        lag(trips, 168) over w as lag_168h,     -- same hour last week
        lag(trips, 336) over w as lag_336h,     -- same hour 2 weeks ago
        (lag(trips, 168) over w + lag(trips, 336) over w
         + lag(trips, 504) over w + lag(trips, 672) over w) / 4.0 as same_hour_4wk_avg,
        avg(trips) over (partition by zone_id order by hour_ts
                         rows between 191 preceding and 24 preceding) as avg_prev_7d
    from demand d
    window w as (partition by zone_id order by hour_ts)
)

select
    l.zone_id,
    l.borough,
    l.is_manhattan_core,
    l.is_airport,
    l.hour_ts,
    to_date(l.hour_ts)        as date_day,
    hour(l.hour_ts)           as hour_of_day,
    dayofweek(l.hour_ts)      as day_of_week,
    dd.is_holiday,
    dd.is_post_cbd_pricing,
    w.temp_c,
    w.precip_mm,
    w.is_wet,
    w.is_freezing,
    l.lag_24h,
    l.lag_48h,
    l.lag_168h,
    l.lag_336h,
    l.same_hour_4wk_avg,
    l.avg_prev_7d,
    l.trips                   -- the TARGET we predict
from lagged l
join {{ ref('dim_date') }} dd on to_date(l.hour_ts) = dd.date_day
left join {{ ref('stg_weather') }} w on l.hour_ts = w.weather_hour
where l.hour_ts >= timestamp'2024-01-29 00:00:00'   -- skip the first 4 weeks (lags need history)
