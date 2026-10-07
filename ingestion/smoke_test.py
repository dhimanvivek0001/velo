"""Phase 0 smoke test: proves Python can reach Databricks and S3."""

import os

import boto3
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

# Read the settings from .env into environment variables
load_dotenv()


def check_databricks() -> None:
    # Connect using the 'velo' profile saved in ~/.databrickscfg
    w = WorkspaceClient(profile=os.environ["DATABRICKS_PROFILE"])

    me = w.current_user.me()
    print(f"[databricks] connected as {me.user_name}")

    volume = os.environ["DATABRICKS_VOLUME_PATH"]
    entries = list(w.files.list_directory_contents(volume))
    print(f"[databricks] volume {volume} reachable, {len(entries)} entries")


def check_s3() -> None:
    # Connect using the 'velo' profile saved in ~/.aws/credentials
    session = boto3.Session(
        profile_name=os.environ["AWS_PROFILE"],
        region_name=os.environ["AWS_REGION"],
    )
    s3 = session.client("s3")

    bucket = os.environ["S3_BUCKET"]
    resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=5)
    print(f"[s3] bucket {bucket} reachable, {resp.get('KeyCount', 0)} objects")


if __name__ == "__main__":
    check_databricks()
    check_s3()
    print("Phase 0 OK")
