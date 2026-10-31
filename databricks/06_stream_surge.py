# Databricks notebook source
spark.sql("drop table if exists velo.silver.stream_zone_5min")
dbutils.fs.rm("/Volumes/velo/bronze/checkpoints/stream_zone_5min", True)
dbutils.fs.rm("/Volumes/velo/bronze/checkpoints/stream_zone_5min_live", True)
print("Reset done: table + both checkpoints removed.")

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE = "/Volumes/velo/landing/raw/stream/ride_requests_live/"
CHECKPOINT = "/Volumes/velo/bronze/checkpoints/stream_zone_5min_live"
TARGET = "velo.silver.stream_zone_5min"

events = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option(
        "cloudFiles.maxFilesPerTrigger", 1
    )  # one file per micro-batch, like a live feed
    .schema("event_time STRING, company STRING, pu_zone_id INT, do_zone_id INT")
    .load(SOURCE)
    .withColumn("event_ts", F.to_timestamp("event_time"))
)

# COMMAND ----------

zone_5min = (
    events.withWatermark("event_ts", "10 minutes")
    .groupBy(F.window("event_ts", "5 minutes").alias("w"), "pu_zone_id")
    .agg(F.count("*").alias("requests"))
    .select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "pu_zone_id",
        "requests",
    )
)

query = (
    zone_5min.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TARGET)
)
query.awaitTermination()
print("Stream finished.")

# COMMAND ----------

print(f"status: {query.status['message']} | exception: {query.exception()}")
print("batch | rows_in | watermark | rows_in_state | rows_written")
for p in query.recentProgress:
    state = p["stateOperators"][0] if p["stateOperators"] else {}
    print(
        p["batchId"],
        "|",
        p["numInputRows"],
        "|",
        p.get("eventTime", {}).get("watermark"),
        "|",
        state.get("numRowsTotal"),
        "|",
        p["sink"].get("numOutputRows"),
    )
print("batches recorded:", len(query.recentProgress))

# COMMAND ----------

spark.sql(f"""
    select count(distinct window_start) as windows,
           sum(requests)                as total_requests,
           min(window_start)            as first_window,
           max(window_start)            as last_window
    from {TARGET}
""").show()

# COMMAND ----------

spark.sql(f"""
create or replace view velo.gold.stream_surge_alerts as
with days as (
    select dayofweek(date_day) as dow, count(*) as n_days
    from velo.gold.dim_date where not is_holiday
    group by dayofweek(date_day)
),
baseline as (
    select f.zone_id, dayofweek(f.date_day) as dow, f.hour_of_day,
           sum(f.trips) / max(d.n_days) / 12 as expected_per_5min
    from velo.gold.fct_zone_hour f
    join days d on dayofweek(f.date_day) = d.dow
    where not f.is_holiday
    group by f.zone_id, dayofweek(f.date_day), f.hour_of_day
)
select s.window_start, s.pu_zone_id, z.zone_name, z.borough, s.requests,
       round(b.expected_per_5min, 1)              as expected,
       round(s.requests / b.expected_per_5min, 1) as ratio
from {TARGET} s
join baseline b
  on s.pu_zone_id = b.zone_id
 and dayofweek(s.window_start) = b.dow
 and hour(s.window_start) = b.hour_of_day
join velo.gold.dim_zone z on s.pu_zone_id = z.zone_id
where s.requests >= 15 and s.requests / b.expected_per_5min >= 2
""")

display(
    spark.sql(
        "select * from velo.gold.stream_surge_alerts order by window_start, ratio desc"
    )
)

# COMMAND ----------

files = (
    spark.read.json(SOURCE)
    .select("event_time", F.col("_metadata.file_name").alias("file"))
    .groupBy("file")
    .agg(
        F.count("*").alias("events"),
        F.min("event_time").alias("min_event"),
        F.max("event_time").alias("max_event"),
    )
    .orderBy("file")
)
display(files)

# COMMAND ----------

truth = (
    spark.read.json(SOURCE)
    .withColumn("event_ts", F.to_timestamp("event_time"))
    .groupBy(F.window("event_ts", "5 minutes").start.alias("window_start"))
    .agg(F.count("*").alias("truth"))
)
streamed = (
    spark.table(TARGET).groupBy("window_start").agg(F.sum("requests").alias("streamed"))
)

display(
    truth.join(streamed, "window_start", "left")
    .withColumn("lost", F.col("truth") - F.coalesce(F.col("streamed"), F.lit(0)))
    .orderBy("window_start")
)
