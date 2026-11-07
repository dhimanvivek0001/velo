-- Raw history for the feature store: one row per zone per hour, actual trips.
-- Complete grid: zero-demand hours are real rows (trips = 0), not missing.
-- Not a feature table itself; feature tables and training labels are built FROM it.
with zones as (
    select zone_id
    from {{ ref('dim_zone') }}
    where borough not in ('EWR', 'N/A', 'Unknown')
),

hours as (
    select explode(sequence(
        timestamp'2024-01-01 00:00:00', timestamp'2025-06-30 23:00:00', interval 1 hour
    )) as hour_ts
)

select
    z.zone_id,
    h.hour_ts,
    coalesce(f.trips, 0) as trips
from zones z
cross join hours h
left join {{ ref('fct_zone_hour') }} f
    on f.zone_id = z.zone_id and f.hour_ts = h.hour_ts
