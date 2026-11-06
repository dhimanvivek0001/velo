-- One row per day per area (CBD vs rest of NYC): the dataset for difference-in-differences.
-- Excludes EWR (New Jersey) and the unknown zones.
select
    f.date_day,
    d.day_name,
    d.day_of_week,
    d.is_holiday,
    d.is_post_cbd_pricing                                    as is_post,
    case when c.is_cbd then 'CBD' else 'Rest of NYC' end     as area,
    sum(f.trips)                                             as trips
from {{ ref('fct_zone_hour') }} f
join {{ ref('dim_date') }} d on f.date_day = d.date_day
join {{ ref('cbd_zones') }} c on f.zone_id = c.zone_id
where f.borough not in ('EWR', 'N/A', 'Unknown')
group by f.date_day, d.day_name, d.day_of_week, d.is_holiday, d.is_post_cbd_pricing,
         case when c.is_cbd then 'CBD' else 'Rest of NYC' end
