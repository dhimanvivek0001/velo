-- One row per TLC taxi zone (265). Renames the seed's columns to snake_case.
select
    cast(LocationID as int)          as zone_id,
    Borough                          as borough,
    Zone                             as zone_name,
    service_zone,
    LocationID in (264, 265)         as is_unknown_zone
from {{ ref('taxi_zone_lookup') }}
