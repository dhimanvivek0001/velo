{{ config(post_hook=[
    "alter table {{ this }} drop constraint if exists fs_weather_hour_pk",
    "alter table {{ this }} alter column weather_hour set not null",
    "alter table {{ this }} add constraint fs_weather_hour_pk primary key (weather_hour)"
]) }}

-- Feature table: HOUR entity (city-wide weather).
-- ASSUMPTION: actual observed weather stands in for a 24h-ahead weather FORECAST.
-- available_at records when a forecast for this hour would have been known.
-- In production this table would store the forecast issued at available_at instead.
select
    weather_hour,
    weather_hour - interval 24 hours as available_at,
    temp_c,
    precip_mm,
    is_wet,
    is_freezing,
    current_timestamp()              as computed_at
from {{ ref('stg_weather') }}
