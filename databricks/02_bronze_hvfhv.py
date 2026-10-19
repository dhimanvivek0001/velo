# Databricks notebook source
# Read every month of trips in one go
path = "/Volumes/velo/landing/raw/hvfhv/"

df = spark.read.option("mergeSchema", "true").parquet(path)

df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

# This is an "action": now Spark really reads the data
monthly = (
    df.groupBy("year", "month")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("trip_miles"), 2).alias("avg_miles"),
        F.round(F.sum("base_passenger_fare") / 1e6, 1).alias("fares_musd"),
        F.sum(F.when(F.col("cbd_congestion_fee") > 0, 1).otherwise(0)).alias(
            "trips_with_cbd_fee"
        ),
    )
    .orderBy("year", "month")
)

display(monthly)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS velo.bronze.checkpoints;

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE = "/Volumes/velo/landing/raw/hvfhv/"
CHECKPOINT = "/Volumes/velo/bronze/checkpoints/hvfhv_trips"
TARGET = "velo.bronze.hvfhv_trips"

bronze_stream = (
    spark.readStream.format("cloudFiles")  # Auto Loader
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", CHECKPOINT)  # remembers the schema it inferred
    .option(
        "cloudFiles.partitionColumns", "year,month"
    )  # turn folder names into columns
    .load(SOURCE)
    .withColumn("_source_file", F.col("_metadata.file_path"))  # lineage: which file
    .withColumn("_ingested_at", F.current_timestamp())  # lineage: when loaded
)

query = (
    bronze_stream.writeStream.option(
        "checkpointLocation", CHECKPOINT
    )  # remembers which files are done
    .trigger(availableNow=True)  # process everything new, then stop
    .toTable(TARGET)
)
query.awaitTermination()
print("Bronze load finished")

# COMMAND ----------

bronze = spark.table("velo.bronze.hvfhv_trips")
print(f"Bronze rows: {bronze.count():,}")
display(bronze.groupBy("_source_file").count().orderBy("_source_file"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY velo.bronze.hvfhv_trips;

# COMMAND ----------

print(f"Bronze rows: {spark.table('velo.bronze.hvfhv_trips').count():,}")

display(
    spark.sql("""
  SELECT version, operation, operationMetrics.numOutputRows AS rows_written
  FROM (DESCRIBE HISTORY velo.bronze.hvfhv_trips)
  ORDER BY version
""")
)

# COMMAND ----------

print(spark.read.option("versionAsOf", 0).table("velo.bronze.hvfhv_trips").count())


# COMMAND ----------
