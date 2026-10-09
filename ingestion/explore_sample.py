"""Step 1.1: profile one month of NYC HVFHV trip data before building pipelines."""

import pandas as pd
import pyarrow.parquet as pq

FILE = "data/sample/fhvhv_tripdata_2025-01.parquet"
SAMPLE_ROWS = 500_000

# Show all columns when printing tables
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

# --- 1. Metadata: read the file's footer, no data loaded ---
pf = pq.ParquetFile(FILE)
print("=" * 60)
print(f"Total rows:      {pf.metadata.num_rows:,}")
print(f"Total columns:   {pf.metadata.num_columns}")
print(f"Row groups:      {pf.metadata.num_row_groups}")
print("=" * 60)
print("SCHEMA (column name: data type)")
print(pf.schema_arrow)

# --- 2. Load a sample, not the whole file ---
first_batch = next(pf.iter_batches(batch_size=SAMPLE_ROWS))
df = first_batch.to_pandas()
print("=" * 60)
print(f"Sample loaded: {len(df):,} rows")
print(df.head(5))

# --- 3. Missing values per column ---
print("=" * 60)
print("NULL % PER COLUMN")
print((df.isna().mean() * 100).round(2).sort_values(ascending=False))

# --- 4. Numeric summary: spot weird values ---
print("=" * 60)
print("NUMERIC SUMMARY")
print(df.describe().T)

# --- 5. Date range check ---
print("=" * 60)
print("PICKUP DATE RANGE")
print(f"min: {df['pickup_datetime'].min()}")
print(f"max: {df['pickup_datetime'].max()}")

# --- 6. Business metric preview: rider wait time ---
df["wait_min"] = (
    df["pickup_datetime"] - df["request_datetime"]
).dt.total_seconds() / 60
print("=" * 60)
print("WAIT TIME (minutes): request -> pickup")
print(df["wait_min"].describe())

# --- 7. Data quality red flags ---
print("=" * 60)
print("RED FLAGS")
print(f"Negative fares:            {(df['base_passenger_fare'] < 0).sum():,}")
print(f"Zero-mile trips:           {(df['trip_miles'] == 0).sum():,}")
print(f"Negative wait times:       {(df['wait_min'] < 0).sum():,}")
print(f"Waits over 60 minutes:     {(df['wait_min'] > 60).sum():,}")
print(
    f"Dropoff before pickup:     {(df['dropoff_datetime'] < df['pickup_datetime']).sum():,}"
)
print(f"Trips with CBD fee > 0:    {(df['cbd_congestion_fee'] > 0).sum():,}")
# --- 8. Better sample: one row group from the MIDDLE of the month ---
mid = pf.metadata.num_row_groups // 2
df_mid = pf.read_row_group(mid).to_pandas()
print("=" * 60)
print(f"MID-MONTH SAMPLE (row group {mid}): {len(df_mid):,} rows")
print(
    f"pickup range: {df_mid['pickup_datetime'].min()} -> {df_mid['pickup_datetime'].max()}"
)

df_mid["wait_min"] = (
    df_mid["pickup_datetime"] - df_mid["request_datetime"]
).dt.total_seconds() / 60
print(f"median wait (mid-month):   {df_mid['wait_min'].median():.2f} min")
print(f"trips with CBD fee > 0:    {(df_mid['cbd_congestion_fee'] > 0).sum():,}")

# --- 9. Test hypothesis: are the nulls coming from one company? ---
print("=" * 60)
print("NULL % BY COMPANY (HV0003 = Uber, HV0005 = Lyft)")
null_by_company = df_mid.groupby("hvfhs_license_num")[
    ["on_scene_datetime", "originating_base_num"]
].apply(lambda g: (g.isna().mean() * 100).round(2))
print(null_by_company)
print("\nTRIPS BY COMPANY")
print(df_mid["hvfhs_license_num"].value_counts())
