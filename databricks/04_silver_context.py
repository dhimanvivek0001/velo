# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as F

# Small data: read with pandas (handles nanosecond timestamps), then convert to Spark
w = pd.read_parquet("/Volumes/velo/landing/raw/weather/")
print(f"weather rows: {len(w):,} | columns: {list(w.columns)}")

# Uniqueness check: exactly one row per hour?
dupes = w["hour_local"].duplicated().sum()
print(f"duplicate hours: {dupes}")

weather = (
    spark.createDataFrame(w.drop(columns=["year", "month"], errors="ignore"))
    .withColumnRenamed("hour_local", "weather_hour")
    .withColumnRenamed("temperature_2m", "temp_c")
    .withColumnRenamed("wind_speed_10m", "wind_kmh")
    .withColumnRenamed("precipitation", "precip_mm")
    .withColumnRenamed("snowfall", "snow_cm")
    .withColumnRenamed("rain", "rain_mm")
    .withColumn("is_wet", F.col("precip_mm") >= 0.5)  # noticeable rain/snow
    .withColumn("is_freezing", F.col("temp_c") <= 0)
    .withColumn(
        "weather_group",  # WMO code -> human label
        F.when(F.col("weather_code") == 0, "clear")
        .when(F.col("weather_code").between(1, 3), "cloudy")
        .when(F.col("weather_code").between(45, 48), "fog")
        .when(F.col("weather_code").between(51, 57), "drizzle")
        .when(F.col("weather_code").between(61, 67), "rain")
        .when(F.col("weather_code").between(71, 77), "snow")
        .when(F.col("weather_code").between(80, 82), "rain_showers")
        .when(F.col("weather_code").between(85, 86), "snow_showers")
        .when(F.col("weather_code") >= 95, "thunderstorm")
        .otherwise("other"),
    )
)

weather.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "velo.silver.weather_hourly"
)
display(
    spark.table("velo.silver.weather_hourly")
    .groupBy("weather_group")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

e = pd.read_parquet("/Volumes/velo/landing/raw/events/")
text_cols = [
    c
    for c in e.columns
    if c not in ("start_date_time", "end_date_time", "year", "month")
]
events_raw = spark.createDataFrame(
    e.drop(columns=["year", "month"], errors="ignore").astype(
        {c: "string" for c in text_cols}
    )
)

events = (
    events_raw.dropDuplicates()  # removes exact duplicate rows only
    .select(
        "event_id",
        "event_name",
        "event_type",
        "event_agency",
        "event_borough",
        "event_location",
        "street_closure_type",
        F.col("start_date_time").alias("start_ts"),
        F.col("end_date_time").alias("end_ts"),
    )
    .withColumn(
        "duration_hours",
        (F.unix_timestamp("end_ts") - F.unix_timestamp("start_ts")) / 3600,
    )
    .withColumn("is_sports_permit", F.col("event_type").startswith("Sport"))
    .withColumn(
        "is_full_closure", F.col("street_closure_type") == "Full Street Closure"
    )
)

events.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "velo.silver.events"
)

s = spark.table("velo.silver.events")
print(
    f"raw {len(e):,} -> silver {s.count():,} (exact duplicates removed: {len(e) - s.count():,})"
)

# Which permits recur the most?
display(
    s.groupBy("event_id", "event_name", "event_type")
    .agg(F.count("*").alias("occurrences"))
    .orderBy(F.desc("occurrences"))
    .limit(10)
)

# The events that plausibly move ride demand
display(
    s.filter(~F.col("is_sports_permit"))
    .groupBy("event_type")
    .agg(
        F.count("*").alias("occurrences"),
        F.sum(F.col("is_full_closure").cast("int")).alias("full_closures"),
    )
    .orderBy(F.desc("occurrences"))
)

# COMMAND ----------
