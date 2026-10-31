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

print(spark.table("velo.bronze.hvfhv_trips").columns[:10])

# COMMAND ----------


DATES = [
    "2025-03-14",
    "2025-01-20",
    "2024-11-28",
]  # different dates -> no result-cache cheating

QUERIES = {
    "bronze (not clustered)": """
        select count(*) from velo.bronze.hvfhv_trips
        where pickup_datetime >= '{d}' and pickup_datetime < date_add('{d}', 1)
          and PULocationID = 161""",
    "silver (clustered)": """
        select count(*) from velo.silver.hvfhv_trips
        where pickup_date = '{d}' and pu_location_id = 161""",
}

results = {}
for label, sql in QUERIES.items():
    times = []
    for d in DATES:
        start = time.perf_counter()
        n = spark.sql(sql.format(d=d)).collect()[0][0]
        times.append(time.perf_counter() - start)
        print(f"{label:<24} {d}  {n:>6,} trips  {times[-1]:5.2f}s")
    results[label] = sum(times) / len(times)

for table in ["velo.bronze.hvfhv_trips", "velo.silver.hvfhv_trips"]:
    files = spark.sql(f"describe detail {table}").select("numFiles").first()[0]
    print(f"{table}: {files:,} files")

speedup = results["bronze (not clustered)"] / results["silver (clustered)"]
print(
    f"\nAverage: bronze {results['bronze (not clustered)']:.2f}s vs "
    f"silver {results['silver (clustered)']:.2f}s  ->  {speedup:.1f}x faster"
)

# COMMAND ----------

import time

DATES = [
    "2025-03-14",
    "2025-01-20",
    "2024-11-28",
    "2024-08-15",
    "2025-05-09",
    "2024-12-31",
]
BRONZE = """select count(*) from velo.bronze.hvfhv_trips
            where pickup_datetime >= '{d}' and pickup_datetime < date_add('{d}', 1)
              and PULocationID = 161"""
SILVER = """select count(*) from velo.silver.hvfhv_trips
            where pickup_date = '{d}' and pu_location_id = 161"""


def run(sql: str, d: str) -> float:
    start = time.perf_counter()
    spark.sql(sql.format(d=d)).collect()
    return time.perf_counter() - start


# 1) Warm-up: touch both tables once (results ignored)
run(BRONZE, "2024-07-04")
run(SILVER, "2024-07-04")

# 2) Interleave: bronze, silver, bronze, silver... on the same dates
b_times, s_times = [], []
for d in DATES:
    b_times.append(run(BRONZE, d))
    s_times.append(run(SILVER, d))
    print(f"{d}   bronze {b_times[-1]:5.2f}s   silver {s_times[-1]:5.2f}s")

b_med = sorted(b_times)[len(b_times) // 2]
s_med = sorted(s_times)[len(s_times) // 2]
print(
    f"\nMedian: bronze {b_med:.2f}s vs silver {s_med:.2f}s  ->  {b_med / s_med:.1f}x faster"
)
