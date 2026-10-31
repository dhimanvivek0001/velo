# Databricks notebook source
from pyspark.sql import functions as F

SOURCE = "/Volumes/velo/landing/raw/stream/ride_requests_v2/"
CHECKPOINT = "/Volumes/velo/bronze/checkpoints/stream_zone_5min_v2"
TARGET = "velo.silver.stream_zone_5min_v2"

spark.sql(f"drop table if exists {TARGET}")
dbutils.fs.rm(CHECKPOINT, True)
print("Reset done.")

# COMMAND ----------

events = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.maxFilesPerTrigger", 1)
    .schema(
        "event_id STRING, schema_version INT, event_time STRING, "
        "company STRING, pu_zone_id INT, do_zone_id INT"
    )
    .load(SOURCE)
    .withColumn("event_ts", F.to_timestamp("event_time"))
)

zone_5min = (
    events.withWatermark("event_ts", "10 minutes")
    .dropDuplicatesWithinWatermark(["event_id"])  # idempotency: retries counted once
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

raw = spark.read.json(SOURCE)
rows = raw.count()
unique_ids = raw.select("event_id").distinct().count()
print(
    f"rows in files: {rows:,} | unique event_ids: {unique_ids:,} | duplicates: {rows - unique_ids:,}"
)

spark.sql(f"""
    select count(distinct window_start) as windows,
           sum(requests)                as total_requests,
           min(window_start)            as first_window,
           max(window_start)            as last_window
    from {TARGET}
""").show()

# COMMAND ----------

truth = (
    raw.dropDuplicates(["event_id"])
    .withColumn("event_ts", F.to_timestamp("event_time"))
    .groupBy(F.window("event_ts", "5 minutes").start.alias("window_start"))
    .agg(F.count("*").alias("truth"))
)
streamed = (
    spark.table(TARGET).groupBy("window_start").agg(F.sum("requests").alias("streamed"))
)

result = (
    truth.join(streamed, "window_start", "left")
    .withColumn(
        "lost_or_extra", F.col("truth") - F.coalesce(F.col("streamed"), F.lit(0))
    )
    .orderBy("window_start")
)
display(result)
