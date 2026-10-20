-- Fails if any zone appears twice in the same hour (grain violated)
select zone_id, hour_ts, count(*) as n
from {{ ref('fct_zone_hour') }}
group by zone_id, hour_ts
having count(*) > 1
