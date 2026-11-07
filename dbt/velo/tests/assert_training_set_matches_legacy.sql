-- Shadow migration check: the feature-store training set must reproduce the legacy
-- ml_zone_hour_features rows and shared values exactly. Returns rows = failures.
select
    coalesce(n.zone_id, o.zone_id) as zone_id,
    coalesce(n.hour_ts, o.hour_ts) as hour_ts
from {{ ref('ml_training_set') }} n
full outer join {{ ref('ml_zone_hour_features') }} o
    on n.zone_id = o.zone_id and n.hour_ts = o.hour_ts
where n.zone_id is null
   or o.zone_id is null
   or n.trips <> o.trips
   or not (n.lag_24h  <=> o.lag_24h)
   or not (n.lag_48h  <=> o.lag_48h)
   or not (n.lag_168h <=> o.lag_168h)
   or not (n.lag_336h <=> o.lag_336h)
   or abs(coalesce(n.same_hour_4wk_avg, 0) - coalesce(cast(o.same_hour_4wk_avg as double), 0)) > 1e-6
   or abs(coalesce(n.avg_prev_7d, 0) - coalesce(cast(o.avg_prev_7d as double), 0)) > 1e-6
   or not (n.temp_c     <=> o.temp_c)
   or not (n.is_holiday <=> o.is_holiday)
