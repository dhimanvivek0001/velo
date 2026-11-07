-- One row per zone per hour in the training set. Returns rows = failures.
select zone_id, hour_ts, count(*) as n
from {{ ref('ml_training_set') }}
group by zone_id, hour_ts
having count(*) > 1
