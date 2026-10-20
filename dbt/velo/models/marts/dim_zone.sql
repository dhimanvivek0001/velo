-- One row per taxi zone, with business flags.
select
    zone_id,
    borough,
    zone_name,
    service_zone,
    is_unknown_zone,
    service_zone = 'Yellow Zone'                                       as is_manhattan_core,
    zone_name in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport') as is_airport
from {{ ref('stg_zones') }}
