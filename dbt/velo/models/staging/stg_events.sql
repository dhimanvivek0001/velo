-- One row per permit occurrence.
select
    event_id,
    event_name,
    event_type,
    event_borough                    as borough,
    event_location,
    start_ts,
    end_ts,
    duration_hours,
    is_sports_permit,
    is_full_closure
from {{ source('silver', 'events') }}
