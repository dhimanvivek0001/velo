{{ config(post_hook=[
    "alter table {{ this }} drop constraint if exists fs_calendar_pk",
    "alter table {{ this }} alter column date_day set not null",
    "alter table {{ this }} add constraint fs_calendar_pk primary key (date_day)"
]) }}

-- Feature table: DATE entity (one row per calendar day).
-- Calendar facts are known in advance, so they are safe to use for a day-ahead forecast.
-- WARNING: raw holiday distances (days_to_next_holiday, days_since_last_holiday) act as a
-- DATE FINGERPRINT for tree models (holidays are irregularly spaced). Use the *_capped
-- versions in models; the raw ones are kept for analysis only.

with d as (
    select
        date_day, day_of_week, day_name, is_weekend, is_holiday, holiday_name, is_post_cbd_pricing,
        (is_weekend or is_holiday) as is_day_off
    from {{ ref('dim_date') }}
),

-- Gaps and islands: consecutive days off share the same block_id
blocks as (
    select
        *,
        date_sub(date_day, cast(row_number() over (partition by is_day_off order by date_day) as int)) as block_id
    from d
),

sized as (
    select *, count(*) over (partition by is_day_off, block_id) as block_len
    from blocks
),

near as (
    select
        *,
        min(case when is_holiday then date_day end)
            over (order by date_day rows between 1 following and unbounded following) as next_holiday,
        max(case when is_holiday then date_day end)
            over (order by date_day rows between unbounded preceding and 1 preceding) as prev_holiday
    from sized
)

select
    date_day,
    day_of_week,
    day_name,
    is_weekend,
    is_holiday,
    holiday_name,
    is_post_cbd_pricing,
    is_day_off,
    (is_day_off and block_len >= 3)                                 as is_long_weekend,
    datediff(next_holiday, date_day)                                as days_to_next_holiday,
    datediff(date_day, prev_holiday)                                as days_since_last_holiday,
    least(coalesce(datediff(next_holiday, date_day), 7), 7)         as days_to_holiday_capped,
    least(coalesce(datediff(date_day, prev_holiday), 7), 7)         as days_since_holiday_capped,
    coalesce(datediff(next_holiday, date_day) = 1, false)           as is_day_before_holiday,
    coalesce(datediff(date_day, prev_holiday) = 1, false)           as is_day_after_holiday,
    current_timestamp()                                             as computed_at
from near
