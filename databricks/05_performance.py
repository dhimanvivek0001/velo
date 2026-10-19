# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE DETAIL velo.silver.hvfhv_trips;

# COMMAND ----------

import time

from pyspark.sql import functions as F


def benchmark(label: str) -> float:
    start = time.time()
    result = (
        spark.table("velo.silver.hvfhv_trips")
        .filter(
            (F.col("pickup_date") == "2025-03-14") & (F.col("pu_location_id") == 161)
        )
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trips"), F.round(F.avg("wait_min"), 2).alias("avg_wait")
        )
        .collect()  # force execution
    )
    secs = time.time() - start
    print(f"{label}: {secs:.1f}s | {len(result)} hours returned")
    return secs


before = benchmark("BEFORE")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Collect stats on the columns we actually filter and join on
# MAGIC ALTER TABLE velo.silver.hvfhv_trips SET TBLPROPERTIES (
# MAGIC   'delta.dataSkippingStatsColumns' =
# MAGIC   'pickup_date,pickup_hour,pu_location_id,do_location_id,pickup_ts,request_ts,company,wait_min,year,month'
# MAGIC );
# MAGIC
# MAGIC -- 2. Recompute stats for the files that already exist
# MAGIC ANALYZE TABLE velo.silver.hvfhv_trips COMPUTE DELTA STATISTICS;
# MAGIC
# MAGIC -- 3. Now clustering is allowed
# MAGIC ALTER TABLE velo.silver.hvfhv_trips CLUSTER BY (pickup_date, pu_location_id);
# MAGIC
# MAGIC -- 4. Rewrite the files, sorted by the clustering columns
# MAGIC OPTIMIZE velo.silver.hvfhv_trips;

# COMMAND ----------

after = benchmark("AFTER")
print(f"Speed-up: {before / after:.1f}x faster")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL velo.silver.hvfhv_trips;

# COMMAND ----------
