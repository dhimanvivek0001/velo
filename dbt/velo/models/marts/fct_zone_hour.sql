-- One row per pickup zone per hour, with demand, service and context metrics.
with trips as (
    select
        pu_location_id                     as zone_id,
        date_trunc('hour', pickup_ts)      as hour_ts,
        company,
        wait_min,
        (not is_bad_wait and not is_wait_outlier) as is_valid_wait,
        base_passenger_fare,
        driver_pay,
        tips,
        trip_miles,
        cbd_congestion_fee
    from {{ ref('stg_trips') }}
),

zone_hour as (
    select
        zone_id,
        hour_ts,
        count(*)                                                        as trips,
        sum(case when company = 'Uber' then 1 else 0 end)               as trips_uber,
        sum(case when company = 'Lyft' then 1 else 0 end)               as trips_lyft,
        percentile_approx(case when is_valid_wait then wait_min end, 0.5) as median_wait_min,
        percentile_approx(case when is_valid_wait then wait_min end, 0.9) as p90_wait_min,
        avg(case when is_valid_wait then
                 case when wait_min > 10 then 1.0 else 0.0 end end)    as share_wait_over_10min,
        round(sum(base_passenger_fare), 2)                              as gross_bookings_usd,
        round(sum(driver_pay), 2)                                       as driver_pay_usd,
        round(sum(tips), 2)                                             as tips_usd,
        round(avg(trip_miles), 2)                                       as avg_trip_miles,
        sum(case when cbd_congestion_fee > 0 then 1 else 0 end)         as trips_with_cbd_fee
    from trips
    group by zone_id, hour_ts
),

event_hours as (
    -- Non-sports events, spread across every hour they were active, per borough
    select
        borough,
        hour_ts,
        count(*)                                  as active_events,
        max(cast(is_full_closure as int)) = 1     as any_full_closure
    from (
        select
            borough,
            is_full_closure,
            explode(sequence(
                date_trunc('hour', start_ts),
                date_trunc('hour', end_ts - interval 1 second),
                interval 1 hour
            )) as hour_ts
        from {{ ref('stg_events') }}
        where not is_sports_permit
          and duration_hours > 0
          and duration_hours <= 72
    )
    group by borough, hour_ts
)

select
    zh.zone_id,
    zh.hour_ts,
    to_date(zh.hour_ts)                          as date_day,
    hour(zh.hour_ts)                             as hour_of_day,
    -- zone context
    z.borough,
    z.zone_name,
    z.is_manhattan_core,
    z.is_airport,
    -- calendar context
    d.day_name,
    d.is_weekend,
    d.is_holiday,
    d.is_post_cbd_pricing,
    -- demand + service + money
    zh.trips,
    zh.trips_uber,
    zh.trips_lyft,
    zh.median_wait_min,
    zh.p90_wait_min,
    zh.share_wait_over_10min,
    zh.gross_bookings_usd,
    zh.driver_pay_usd,
    zh.tips_usd,
    zh.avg_trip_miles,
    zh.trips_with_cbd_fee,
    -- weather context
    w.temp_c,
    w.precip_mm,
    w.weather_group,
    w.is_wet,
    w.is_freezing,
    -- event context
    coalesce(e.active_events, 0)                 as borough_active_events,
    coalesce(e.any_full_closure, false)          as borough_any_full_closure
from zone_hour zh
left join {{ ref('dim_zone') }}    z on zh.zone_id = z.zone_id
left join {{ ref('dim_date') }}    d on to_date(zh.hour_ts) = d.date_day
left join {{ ref('stg_weather') }} w on zh.hour_ts = w.weather_hour
left join event_hours              e on z.borough = e.borough and zh.hour_ts = e.hour_ts
