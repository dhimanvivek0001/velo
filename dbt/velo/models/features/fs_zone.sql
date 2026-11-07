{{ config(post_hook=[
    "alter table {{ this }} drop constraint if exists fs_zone_pk",
    "alter table {{ this }} alter column zone_id set not null",
    "alter table {{ this }} add constraint fs_zone_pk primary key (zone_id)"
]) }}

-- Feature table: ZONE entity (one row per pickup zone).
-- Static attributes + a demand profile. The profile uses the TRAINING period only
-- (before 2025-03-01), so validation and test months never leak into the features.

with attrs as (
    select zone_id, borough, is_manhattan_core, is_airport
    from {{ ref('dim_zone') }}
    where borough not in ('EWR', 'N/A', 'Unknown')
),

hist as (
    select zone_id, hour(hour_ts) as hour_of_day, dayofweek(hour_ts) as day_of_week, trips
    from {{ ref('zone_hour_observations') }}
    where hour_ts < timestamp'2025-03-01 00:00:00'
)

select
    a.zone_id,
    a.borough,
    a.is_manhattan_core,
    a.is_airport,
    avg(h.trips)                                                      as profile_avg_hourly_trips,
    sum(case when h.hour_of_day < 6 then h.trips else 0 end)
        / nullif(sum(h.trips), 0)                                     as profile_night_share,
    avg(case when h.trips = 0 then 1.0 else 0.0 end)                  as profile_zero_hour_share,
    avg(case when h.day_of_week in (1, 7) then h.trips end)           -- Spark: 1 = Sunday, 7 = Saturday
        / nullif(avg(case when h.day_of_week not in (1, 7) then h.trips end), 0)
                                                                      as profile_weekend_lift,
    timestamp'2025-03-01 00:00:00'                                    as profile_as_of,
    current_timestamp()                                               as computed_at
from attrs a
left join hist h on a.zone_id = h.zone_id
group by all
