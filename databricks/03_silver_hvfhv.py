# Databricks notebook source
from pyspark.sql import functions as F

bronze = spark.table("velo.bronze.hvfhv_trips")

COMPANY = {"HV0002": "Juno", "HV0003": "Uber", "HV0004": "Via", "HV0005": "Lyft"}
company_map = F.create_map([F.lit(x) for kv in COMPANY.items() for x in kv])


def yn(col):
    """'Y'/'N' text -> true/false (null stays null)."""
    return F.when(F.col(col) == "Y", True).when(F.col(col) == "N", False)


typed = (
    bronze.select(
        # --- keys and renames ---
        F.col("hvfhs_license_num").alias("license_num"),
        company_map[F.col("hvfhs_license_num")].alias("company"),
        F.col("dispatching_base_num").alias("dispatching_base"),
        F.col("request_datetime").alias("request_ts"),
        F.col("on_scene_datetime").alias("on_scene_ts"),
        F.col("pickup_datetime").alias("pickup_ts"),
        F.col("dropoff_datetime").alias("dropoff_ts"),
        F.col("PULocationID").cast("int").alias("pu_location_id"),
        F.col("DOLocationID").cast("int").alias("do_location_id"),
        "trip_miles",
        F.col("trip_time").alias("trip_time_sec"),
        # --- money ---
        "base_passenger_fare",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "tips",
        "driver_pay",
        F.coalesce(F.col("cbd_congestion_fee"), F.lit(0.0)).alias("cbd_congestion_fee"),
        # --- Y/N flags -> booleans ---
        yn("shared_request_flag").alias("is_shared_request"),
        yn("shared_match_flag").alias("is_shared_match"),
        yn("access_a_ride_flag").alias("is_access_a_ride"),
        yn("wav_request_flag").alias("is_wav_request"),
        yn("wav_match_flag").alias("is_wav_match"),
        # --- partition + lineage ---
        "year",
        "month",
        "_source_file",
        "_ingested_at",
    )
    # --- derived fields (try_divide returns null instead of crashing on /0) ---
    .withColumn(
        "wait_min",
        (F.unix_timestamp("pickup_ts") - F.unix_timestamp("request_ts")) / 60,
    )
    .withColumn("trip_min", F.col("trip_time_sec") / 60)
    .withColumn(
        "speed_mph", F.try_divide(F.col("trip_miles"), F.col("trip_time_sec") / 3600)
    )
    .withColumn(
        "fare_per_mile", F.try_divide(F.col("base_passenger_fare"), F.col("trip_miles"))
    )
    .withColumn(
        "driver_take_rate",
        F.try_divide(F.col("driver_pay"), F.col("base_passenger_fare")),
    )
    .withColumn("pickup_date", F.to_date("pickup_ts"))
    .withColumn("pickup_hour", F.hour("pickup_ts"))
    # --- hard-failure reason (null = row is fine) ---
    .withColumn(
        "_reject_reason",
        F.when(F.col("pickup_ts").isNull(), "missing_pickup")
        .when(F.col("dropoff_ts") < F.col("pickup_ts"), "dropoff_before_pickup")
        .when(
            (F.year("pickup_ts") != F.col("year"))
            | (F.month("pickup_ts") != F.col("month")),
            "pickup_outside_file_month",
        ),
    )
    # --- soft-issue flags (row kept, but marked) ---
    .withColumn("is_bad_wait", F.col("wait_min") < 0)
    .withColumn("is_wait_outlier", F.col("wait_min") > 60)
    .withColumn("is_zero_mile", F.col("trip_miles") == 0)
    .withColumn(
        "is_adjustment", (F.col("base_passenger_fare") < 0) | (F.col("driver_pay") < 0)
    )
    .withColumn(
        "is_unknown_zone",
        F.col("pu_location_id").isin(264, 265) | F.col("do_location_id").isin(264, 265),
    )
    .withColumn("is_impossible_speed", F.col("speed_mph") > 80)
)

silver = typed.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
quarantine = typed.filter(F.col("_reject_reason").isNotNull())

silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "velo.silver.hvfhv_trips"
)
quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "velo.silver.hvfhv_trips_quarantine"
)
print("Silver + quarantine written")

# COMMAND ----------

b = spark.table("velo.bronze.hvfhv_trips").count()
s = spark.table("velo.silver.hvfhv_trips").count()
q = spark.table("velo.silver.hvfhv_trips_quarantine").count()
print(
    f"bronze {b:,} = silver {s:,} + quarantine {q:,}  ->  {'OK' if b == s + q else 'MISMATCH'}"
)

display(
    spark.table("velo.silver.hvfhv_trips_quarantine").groupBy("_reject_reason").count()
)

display(
    spark.table("velo.silver.hvfhv_trips").agg(
        *[
            F.round(F.avg(F.col(c).cast("int")) * 100, 3).alias(f"{c}_pct")
            for c in [
                "is_bad_wait",
                "is_wait_outlier",
                "is_zero_mile",
                "is_adjustment",
                "is_unknown_zone",
                "is_impossible_speed",
            ]
        ]
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Keep data-skipping stats on the columns we filter by, and cluster for date+zone queries
# MAGIC ALTER TABLE velo.silver.hvfhv_trips SET TBLPROPERTIES (
# MAGIC   'delta.dataSkippingStatsColumns' =
# MAGIC   'pickup_date,pickup_hour,pu_location_id,do_location_id,pickup_ts,request_ts,company,wait_min,year,month'
# MAGIC );
# MAGIC ALTER TABLE velo.silver.hvfhv_trips CLUSTER BY (pickup_date, pu_location_id);

# COMMAND ----------
