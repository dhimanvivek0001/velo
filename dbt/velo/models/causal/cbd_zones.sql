-- Which pickup zones are INSIDE the congestion zone?
-- A trip that starts inside the zone is always charged, so inside-zone pickups
-- have ~100% fee share after 5 Jan 2025. We let the data define the zone.
select
    zone_id,
    zone_name,
    borough,
    sum(trips)                                         as trips_post,
    sum(trips_with_cbd_fee)                            as fee_trips_post,
    round(sum(trips_with_cbd_fee) / sum(trips), 3)     as fee_share_post,
    sum(trips_with_cbd_fee) / sum(trips) >= 0.95       as is_cbd
from {{ ref('fct_zone_hour') }}
where is_post_cbd_pricing
group by zone_id, zone_name, borough
