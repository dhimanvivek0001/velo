"""Replay real HVFHV ride requests into Kafka as a live event stream.

Example:
    python streaming/producer.py --month 2025-03 --start "2025-03-14 17:00" --hours 3 --speed 60
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import requests
from confluent_kafka import Producer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
TLC_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{month}.parquet"
)
TOPIC = "ride_requests"
COMPANIES = {"HV0003": "Uber", "HV0005": "Lyft"}
# Only fields known at the moment a rider requests a ride
COLUMNS = ["hvfhs_license_num", "request_datetime", "PULocationID", "DOLocationID"]


def ensure_file(month: str) -> Path:
    """Use the local monthly file if present, otherwise download it once."""
    path = RAW_DIR / f"fhvhv_tripdata_{month}.parquet"
    if path.exists():
        print(f"Using local file {path}")
        return path

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    url = TLC_URL.format(month=month)
    print(f"Downloading {url} (one-time, ~500 MB)...")
    tmp = path.with_suffix(".part")
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as f:
            f.writelines(resp.iter_content(chunk_size=8 * 1024 * 1024))
    tmp.replace(path)  # atomic: no half-written file if interrupted
    return path


def load_requests(path: Path, start: pd.Timestamp, hours: float) -> pd.DataFrame:
    """Read only the requested time window and columns (fast, low memory)."""
    end = start + pd.Timedelta(hours=hours)
    table = pq.read_table(
        path,
        columns=COLUMNS,
        filters=[
            ("request_datetime", ">=", start.to_pydatetime()),
            ("request_datetime", "<", end.to_pydatetime()),
        ],
    )
    df = table.to_pandas()
    return df.sort_values("request_datetime").reset_index(drop=True)


def replay(df: pd.DataFrame, speed: float, bootstrap: str) -> None:
    """Send each request to Kafka, paced by its real timestamp divided by speed."""
    producer = Producer(
        {"bootstrap.servers": bootstrap, "acks": "all", "linger.ms": 50}
    )
    stats = {"failed": 0}

    def on_delivery(err, _msg) -> None:
        if err is not None:
            stats["failed"] += 1

    first_event = df["request_datetime"].iloc[0]
    wall_start = time.monotonic()
    sent = 0

    try:
        for row in df.itertuples(index=False):
            # Wait until this event's (sped-up) moment arrives
            due = (row.request_datetime - first_event).total_seconds() / speed
            delay = due - (time.monotonic() - wall_start)
            if delay > 0:
                time.sleep(delay)

            event = {
                "event_time": row.request_datetime.isoformat(),
                "company": COMPANIES.get(row.hvfhs_license_num, "Other"),
                "pu_zone_id": int(row.PULocationID),
                "do_zone_id": int(row.DOLocationID),
            }
            while True:
                try:
                    producer.produce(
                        TOPIC,
                        key=str(event["pu_zone_id"]),
                        value=json.dumps(event),
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:
                    producer.poll(1)  # local queue full: let Kafka catch up
            producer.poll(0)
            sent += 1

            if sent % 10_000 == 0:
                print(f"  sent {sent:,} | simulated time {row.request_datetime}")
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        producer.flush(30)
        elapsed = time.monotonic() - wall_start
        print(f"Done: sent {sent:,} events in {elapsed:.0f}s, failed {stats['failed']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay TLC ride requests into Kafka")
    parser.add_argument("--month", default="2025-03")
    parser.add_argument("--start", default="2025-03-14 17:00")
    parser.add_argument("--hours", type=float, default=3)
    parser.add_argument(
        "--speed", type=float, default=60, help="60 = 1 real minute per second"
    )
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

    path = ensure_file(args.month)
    df = load_requests(path, pd.Timestamp(args.start), args.hours)
    print(f"Loaded {len(df):,} ride requests from {args.start} (+{args.hours}h)")
    replay(df, args.speed, args.bootstrap)


if __name__ == "__main__":
    main()
