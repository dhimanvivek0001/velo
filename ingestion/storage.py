"""Shared helper: land small files in S3 (raw archive) + Databricks (landing volume)."""

import logging
import os
from pathlib import Path

import boto3
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

log = logging.getLogger(__name__)


def upload_small_file(path: Path, rel_path: str) -> None:
    """Upload a small file to S3 raw/ and the Databricks landing volume.

    rel_path example: "events/year=2025/month=01/events_nyc_2025-01.parquet"
    Small files are simply overwritten: re-uploading is cheaper than checking first.
    """
    session = boto3.Session(
        profile_name=os.environ["AWS_PROFILE"], region_name=os.environ["AWS_REGION"]
    )
    bucket = os.environ["S3_BUCKET"]
    key = f"raw/{rel_path}"
    session.client("s3").upload_file(str(path), bucket, key)
    log.info("Uploaded to s3://%s/%s", bucket, key)

    w = WorkspaceClient(profile=os.environ["DATABRICKS_PROFILE"])
    target = f"{os.environ['DATABRICKS_VOLUME_PATH']}/{rel_path}"
    w.files.create_directory(target.rsplit("/", 1)[0])
    with open(path, "rb") as f:
        w.files.upload(target, f, overwrite=True)
    log.info("Uploaded to Databricks %s", target)
