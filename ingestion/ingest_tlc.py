"""Ingest one month of NYC TLC HVFHV trip data: download -> validate -> upload."""

import argparse
import logging
import os
import threading
from pathlib import Path

import boto3
import pyarrow.parquet as pq
import requests
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config as DatabricksConfig
from databricks.sdk.errors import NotFound
from dotenv import load_dotenv

load_dotenv()

# ---------- Settings ----------
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATASET = "fhvhv"
LOCAL_DIR = Path("data/raw")
MIN_ROWS = 1_000_000
REQUIRED_COLUMNS = {
    "hvfhs_license_num",
    "request_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "base_passenger_fare",
    "driver_pay",
}

# ---------- Logging: timestamped messages instead of print ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ingest_tlc")


def file_name(year: int, month: int) -> str:
    """fhvhv_tripdata_2025-01.parquet"""
    return f"{DATASET}_tripdata_{year}-{month:02d}.parquet"


def build_url(year: int, month: int) -> str:
    return f"{BASE_URL}/{file_name(year, month)}"


def partition_path(year: int, month: int) -> str:
    """hvfhv/year=2025/month=01/fhvhv_tripdata_2025-01.parquet"""
    return f"hvfhv/year={year}/month={month:02d}/{file_name(year, month)}"


# ---------- Step 1: Download ----------
def download(year: int, month: int) -> Path:
    """Download the month's file. Skips if it already exists (idempotent)."""
    dest = LOCAL_DIR / file_name(year, month)

    if dest.exists():
        log.info("Already downloaded, skipping: %s", dest)
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")
    url = build_url(year, month)
    log.info("Downloading %s", url)

    # stream=True: download in chunks instead of loading 468 MB into memory
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(tmp, "wb") as f:
            f.writelines(response.iter_content(chunk_size=8 * 1024 * 1024))

    # Rename only after the download fully succeeded
    tmp.rename(dest)
    log.info("Saved %s (%.1f MB)", dest, dest.stat().st_size / 1024**2)
    return dest


# ---------- Step 2: Validate ----------
def validate(path: Path, year: int, month: int) -> int:
    """Check the file is usable. Raise on structural problems, warn on content oddities."""
    pf = pq.ParquetFile(path)
    rows = pf.metadata.num_rows

    # 1. Structural checks: these FAIL the pipeline
    missing = REQUIRED_COLUMNS - set(pf.schema_arrow.names)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    if rows < MIN_ROWS:
        raise ValueError(f"Only {rows:,} rows, expected at least {MIN_ROWS:,}")

    # 2. Content check: pickup dates should fall in the requested month.
    #    Uses row-group statistics from the footer, so no data is loaded.
    col = pf.schema_arrow.get_field_index("pickup_datetime")
    mins, maxs = [], []
    for i in range(pf.metadata.num_row_groups):
        stats = pf.metadata.row_group(i).column(col).statistics
        if stats is not None and stats.has_min_max:
            mins.append(stats.min)
            maxs.append(stats.max)

    if mins:
        first, last = min(mins), max(maxs)
        log.info("Pickup range: %s -> %s", first, last)
        expected = (year, month)
        if (first.year, first.month) != expected or (last.year, last.month) != expected:
            log.warning("Some pickups fall outside %d-%02d", year, month)

    log.info(
        "Validation passed: %s rows, %d columns",
        f"{rows:,}",
        len(pf.schema_arrow.names),
    )
    return rows


# ---------- Progress logging (shared by both uploads) ----------
class UploadProgress:
    """Logs upload progress every 10%. Uploaders call this from several threads."""

    def __init__(self, total_bytes: int, label: str):
        self.total = total_bytes
        self.label = label
        self.sent = 0
        self.next_pct = 10
        self.lock = threading.Lock()

    def __call__(self, bytes_sent: int) -> None:
        with self.lock:
            self.sent += bytes_sent
            pct = self.sent * 100 / self.total
            if pct >= self.next_pct:
                log.info(
                    "%s upload %d%% (%.0f / %.0f MB)",
                    self.label,
                    pct,
                    self.sent / 1024**2,
                    self.total / 1024**2,
                )
                self.next_pct += 10


# ---------- Step 3: Upload to S3 (raw archive) ----------
# Network settings tuned for a slow, shared home connection
S3_CLIENT_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "adaptive"},
    connect_timeout=30,
    read_timeout=300,
)
S3_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=16 * 1024**2,  # use multipart for files > 16 MB
    multipart_chunksize=16 * 1024**2,  # each part = 16 MB
    max_concurrency=2,  # only 2 parallel connections (default is 10)
)


def upload_to_s3(path: Path, year: int, month: int) -> str:
    """Upload to the S3 raw archive. Skips if an identical-size copy exists."""
    session = boto3.Session(
        profile_name=os.environ["AWS_PROFILE"],
        region_name=os.environ["AWS_REGION"],
    )
    s3 = session.client("s3", config=S3_CLIENT_CONFIG)
    bucket = os.environ["S3_BUCKET"]
    key = f"raw/{partition_path(year, month)}"
    local_size = path.stat().st_size

    try:
        remote = s3.head_object(Bucket=bucket, Key=key)
        if remote["ContentLength"] == local_size:
            log.info("S3 already has identical file, skipping: s3://%s/%s", bucket, key)
            return key
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise

    log.info("Uploading to s3://%s/%s (%.0f MB)", bucket, key, local_size / 1024**2)
    s3.upload_file(
        str(path),
        bucket,
        key,
        Config=S3_TRANSFER_CONFIG,
        Callback=UploadProgress(local_size, "S3"),
    )
    log.info("S3 upload complete")
    return key


# ---------- Step 4: Upload to Databricks (landing volume) ----------
class ProgressReader:
    """Wraps a file so we can log progress as the SDK reads it.

    read() is intercepted to count bytes. Every other method the SDK needs
    (seekable, seek, tell) is passed straight through to the real file.
    """

    def __init__(self, f, progress: UploadProgress):
        self._f = f
        self._progress = progress

    def read(self, size: int = -1) -> bytes:
        chunk = self._f.read(size)
        if chunk:
            self._progress(len(chunk))
        return chunk

    def __getattr__(self, name):
        # Only called for attributes ProgressReader doesn't define itself
        return getattr(self._f, name)


def upload_to_databricks(path: Path, year: int, month: int) -> str:
    """Upload to the Unity Catalog volume. Skips if an identical-size copy exists."""
    cfg = DatabricksConfig(
        profile=os.environ["DATABRICKS_PROFILE"],
        retry_timeout_seconds=1800,  # 30 min per part (default 5 min)
        files_ext_network_transfer_inactivity_timeout_seconds=300,  # tolerate 5 min stalls
    )
    w = WorkspaceClient(config=cfg)
    target = f"{os.environ['DATABRICKS_VOLUME_PATH']}/{partition_path(year, month)}"
    local_size = path.stat().st_size

    try:
        meta = w.files.get_metadata(target)
        if meta.content_length == local_size:
            log.info("Databricks already has identical file, skipping: %s", target)
            return target
    except NotFound:
        pass

    w.files.create_directory(target.rsplit("/", 1)[0])
    log.info(
        "Uploading to Databricks volume %s (%.0f MB)", target, local_size / 1024**2
    )
    with open(path, "rb") as f:
        w.files.upload(
            target,
            ProgressReader(f, UploadProgress(local_size, "Databricks")),
            overwrite=True,
            parallelism=2,  # 2 parallel parts (default 10)
        )
    log.info("Databricks upload complete")
    return target


# ---------- Run all steps ----------
def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest one month of TLC HVFHV data")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    path = download(args.year, args.month)
    validate(path, args.year, args.month)
    upload_to_s3(path, args.year, args.month)
    upload_to_databricks(path, args.year, args.month)
    log.info("Done: %d-%02d", args.year, args.month)


if __name__ == "__main__":
    main()
