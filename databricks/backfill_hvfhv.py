# Databricks notebook source
"""Backfill NYC TLC HVFHV trips straight into the Unity Catalog volume.

Databricks downloads directly from the TLC website (fast data-centre network),
instead of routing 5+ GB through a slow home connection.
Idempotent: months already present with the correct size are skipped.
"""

import os
import shutil
import time
import urllib.request

import pyarrow.parquet as pq

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
VOLUME = "/Volumes/velo/landing/raw/hvfhv"

# Study window: Jul 2024 - Jun 2025 (Jan 2025 already loaded)
MONTHS = [(2024, m) for m in range(7, 13)] + [(2025, m) for m in range(1, 7)]


def remote_size(url: str) -> int:
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=60) as r:
        return int(r.headers["Content-Length"])


def ingest_month(year: int, month: int) -> None:
    name = f"fhvhv_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{name}"
    folder = f"{VOLUME}/year={year}/month={month:02d}"
    dest = f"{folder}/{name}"

    expected = remote_size(url)

    # Idempotency: skip if an identical-size file already exists
    if os.path.exists(dest) and os.path.getsize(dest) == expected:
        print(
            f"{year}-{month:02d}  skip (already present, {expected / 1024**2:.0f} MB)"
        )
        return

    os.makedirs(folder, exist_ok=True)
    tmp = dest + ".part"
    start = time.time()
    with urllib.request.urlopen(url, timeout=300) as r, open(tmp, "wb") as f:
        shutil.copyfileobj(r, f, length=16 * 1024 * 1024)  # stream in 16 MB chunks
    os.replace(tmp, dest)  # rename only after the download fully succeeded

    # Validate: size matches + Parquet footer is readable
    size = os.path.getsize(dest)
    if size != expected:
        raise ValueError(f"{name}: size {size} != expected {expected}")
    rows = pq.ParquetFile(dest).metadata.num_rows
    print(
        f"{year}-{month:02d}  ✅ {size / 1024**2:.0f} MB | {rows:,} rows | {time.time() - start:.0f}s"
    )


for y, m in MONTHS:
    ingest_month(y, m)

print("Backfill complete")
