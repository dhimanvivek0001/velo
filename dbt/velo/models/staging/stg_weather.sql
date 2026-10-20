-- One row per local clock hour.
select
    cast(weather_hour as timestamp)  as weather_hour,
    temp_c,
    precip_mm,
    rain_mm,
    snow_cm,
    wind_kmh,
    weather_code,
    weather_group,
    is_wet,
    is_freezing
from {{ source('silver', 'weather_hourly') }}
