"""Ingest one month of hourly NYC weather from the Open-Meteo archive API.

fetch (API) -> validate -> save Parquet -> upload to S3 + Databricks
"""

import argparse
import calendar
import logging
import os
from pathlib import Path

import boto3
import pandas as pd
import requests
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

# ---------- Settings ----------
API_URL = "https://archive-api.open-meteo.com/v1/archive"
NYC_LAT, NYC_LON = 40.7128, -74.0060  # Manhattan
TIMEZONE = "America/New_York"  # match TLC timestamps (NYC local time)
HOURLY_VARS = [
    "temperature_2m",  # air temperature, °C
    "precipitation",  # total rain + snow, mm
    "rain",  # mm
    "snowfall",  # cm
    "wind_speed_10m",  # km/h
    "weather_code",  # WMO code: 0 = clear, 61 = rain, 71 = snow, ...
]
LOCAL_DIR = PROJECT_ROOT / "data" / "raw" / "weather"
MAX_NULL_PCT = 5.0

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("ingest_weather")


def file_name(year: int, month: int) -> str:
    return f"weather_nyc_{year}-{month:02d}.parquet"


def partition_path(year: int, month: int) -> str:
    """weather/year=2025/month=01/weather_nyc_2025-01.parquet"""
    return f"weather/year={year}/month={month:02d}/{file_name(year, month)}"


def make_session() -> requests.Session:
    """HTTP session that retries on rate limits and server errors, with backoff."""
    retry = Retry(
        total=5,  # up to 5 retries
        backoff_factor=2,  # wait 2s, 4s, 8s, 16s... between tries
        status_forcelist=[429, 500, 502, 503, 504],  # 429 = rate limited
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


# ---------- Step 1: Fetch from the API ----------
def fetch(year: int, month: int) -> pd.DataFrame:
    last_day = calendar.monthrange(year, month)[1]
    params = {
        "latitude": NYC_LAT,
        "longitude": NYC_LON,
        "start_date": f"{year}-{month:02d}-01",
        "end_date": f"{year}-{month:02d}-{last_day:02d}",
        "hourly": ",".join(HOURLY_VARS),
        "timezone": TIMEZONE,
    }
    log.info("Calling %s for %d-%02d", API_URL, year, month)
    response = make_session().get(API_URL, params=params, timeout=60)
    response.raise_for_status()

    hourly = response.json()["hourly"]
    df = pd.DataFrame(hourly).rename(columns={"time": "hour_local"})
    df["hour_local"] = pd.to_datetime(df["hour_local"])
    log.info("Received %d hourly rows", len(df))
    return df


# ---------- Step 2: Validate ----------
def validate(df: pd.DataFrame, year: int, month: int) -> None:
    expected = calendar.monthrange(year, month)[1] * 24
    # allow +-1 hour for daylight-saving changes (March and November)
    if abs(len(df) - expected) > 1:
        raise ValueError(f"Expected ~{expected} hourly rows, got {len(df)}")

    missing = set(HOURLY_VARS) - set(df.columns)
    if missing:
        raise ValueError(f"API response missing columns: {sorted(missing)}")

    null_pct = df[HOURLY_VARS].isna().mean() * 100
    for col, pct in null_pct.items():
        if pct > MAX_NULL_PCT:
            log.warning("%s is %.1f%% null", col, pct)

    log.info(
        "Validation passed: %d rows | temp %.1f..%.1f °C | total rain %.1f mm",
        len(df),
        df["temperature_2m"].min(),
        df["temperature_2m"].max(),
        df["rain"].sum(),
    )


# ---------- Step 3: Save locally ----------
def save(df: pd.DataFrame, year: int, month: int) -> Path:
    dest = LOCAL_DIR / file_name(year, month)
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, index=False)
    log.info("Saved %s (%.1f KB)", dest, dest.stat().st_size / 1024)
    return dest


# ---------- Step 4: Upload (small file, so overwrite instead of size-check) ----------
def upload(path: Path, year: int, month: int) -> None:
    session = boto3.Session(
        profile_name=os.environ["AWS_PROFILE"], region_name=os.environ["AWS_REGION"]
    )
    key = f"raw/{partition_path(year, month)}"
    session.client("s3").upload_file(str(path), os.environ["S3_BUCKET"], key)
    log.info("Uploaded to s3://%s/%s", os.environ["S3_BUCKET"], key)

    w = WorkspaceClient(profile=os.environ["DATABRICKS_PROFILE"])
    target = f"{os.environ['DATABRICKS_VOLUME_PATH']}/{partition_path(year, month)}"
    w.files.create_directory(target.rsplit("/", 1)[0])
    with open(path, "rb") as f:
        w.files.upload(target, f, overwrite=True)
    log.info("Uploaded to Databricks %s", target)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest one month of NYC hourly weather"
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    df = fetch(args.year, args.month)
    validate(df, args.year, args.month)
    path = save(df, args.year, args.month)
    upload(path, args.year, args.month)
    log.info("Done: weather %d-%02d", args.year, args.month)


if __name__ == "__main__":
    main()
