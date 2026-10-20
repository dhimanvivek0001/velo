-- One row per calendar day in the study window (Jul 2024 - Jun 2025).
with days as (
    select explode(sequence(date'2024-07-01', date'2025-06-30', interval 1 day)) as date_day
),

us_holidays as (
    select * from values
        (date'2024-07-04', 'Independence Day'),
        (date'2024-09-02', 'Labor Day'),
        (date'2024-10-14', 'Columbus Day'),
        (date'2024-11-11', 'Veterans Day'),
        (date'2024-11-28', 'Thanksgiving'),
        (date'2024-12-25', 'Christmas Day'),
        (date'2025-01-01', 'New Year''s Day'),
        (date'2025-01-20', 'Martin Luther King Jr. Day'),
        (date'2025-02-17', 'Presidents Day'),
        (date'2025-05-26', 'Memorial Day'),
        (date'2025-06-19', 'Juneteenth')
    as h(holiday_date, holiday_name)
)

select
    d.date_day,
    year(d.date_day)                          as year,
    month(d.date_day)                         as month,
    date_format(d.date_day, 'MMM')            as month_name,
    dayofweek(d.date_day)                     as day_of_week,   -- 1 = Sunday ... 7 = Saturday
    date_format(d.date_day, 'EEEE')           as day_name,
    dayofweek(d.date_day) in (1, 7)           as is_weekend,
    h.holiday_name,
    h.holiday_name is not null                as is_holiday,
    d.date_day >= date'2025-01-05'            as is_post_cbd_pricing
from days d
left join us_holidays h
    on d.date_day = h.holiday_date
