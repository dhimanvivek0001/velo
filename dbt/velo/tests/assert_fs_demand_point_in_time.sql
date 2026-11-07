-- Leakage guard for fs_zone_hour_demand. Returns rows = failures.
-- 1) available_at must be exactly 24h before the target hour
-- 2) lag_24h must equal the actual observation AT available_at (the newest value allowed)
with d as (select * from {{ ref('fs_zone_hour_demand') }}),
     o as (select * from {{ ref('zone_hour_observations') }})

select d.zone_id, d.hour_ts, 'available_at is not hour_ts - 24h' as reason
from d
where d.available_at <> d.hour_ts - interval 24 hours

union all

select d.zone_id, d.hour_ts, 'lag_24h differs from observation at available_at' as reason
from d
join o on o.zone_id = d.zone_id and o.hour_ts = d.available_at
where d.lag_24h <> o.trips
