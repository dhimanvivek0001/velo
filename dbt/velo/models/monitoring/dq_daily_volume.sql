-- Daily trip volume vs the same weekday over the previous 4 weeks.
-- Flags days more than 25% away from their baseline, then marks which
-- anomalies are already explained (holiday or a known event).
with daily as (
    select date_day, sum(trips) as trips
    from {{ ref('fct_zone_hour') }}
    group by date_day
),

with_baseline as (
    select
        d.date_day,
        d.trips,
        avg(p.trips)    as baseline_trips,
        stddev(p.trips) as baseline_std,
        count(p.trips)  as baseline_days
    from daily d
    left join daily p
        on p.date_day in (
            date_sub(d.date_day, 7),
            date_sub(d.date_day, 14),
            date_sub(d.date_day, 21),
            date_sub(d.date_day, 28)
        )
    group by d.date_day, d.trips
),

flagged as (
    select
        b.date_day,
        dd.day_name,
        dd.is_holiday,
        dd.holiday_name,
        ke.event_name                                                       as known_event,
        b.trips,
        round(b.baseline_trips, 0)                                          as baseline_trips,
        round((b.trips - b.baseline_trips) / b.baseline_trips, 3)           as pct_vs_baseline,
        round((b.trips - b.baseline_trips) / nullif(b.baseline_std, 0), 2)  as z_score,
        b.baseline_days = 4
            and abs((b.trips - b.baseline_trips) / b.baseline_trips) > 0.25 as is_anomaly
    from with_baseline b
    join {{ ref('dim_date') }} dd on b.date_day = dd.date_day
    left join {{ ref('known_events') }} ke
        on b.date_day between ke.event_start and ke.event_end
)

select
    *,
    is_anomaly and not is_holiday and known_event is null as is_unexplained_anomaly
from flagged
