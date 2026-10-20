-- Fails if trips were lost or double-counted when aggregating to zone-hour
with fact as (select sum(trips) as n from {{ ref('fct_zone_hour') }}),
     src  as (select count(*)   as n from {{ ref('stg_trips') }})
select fact.n as fact_trips, src.n as source_trips
from fact cross join src
where fact.n != src.n
