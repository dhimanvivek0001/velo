-- Known US long weekends must be flagged in fs_calendar.
-- Memorial Day 2024 and 2025, Labor Day 2024. Returns rows = failures.
with expected as (
    select explode(array(
        date'2024-05-25', date'2024-05-26', date'2024-05-27',
        date'2024-08-31', date'2024-09-01', date'2024-09-02',
        date'2025-05-24', date'2025-05-25', date'2025-05-26'
    )) as date_day
)

select e.date_day
from expected e
left join {{ ref('fs_calendar') }} c on e.date_day = c.date_day
where not coalesce(c.is_long_weekend, false)
