"""Replay real HVFHV ride requests into Kafka as a live event stream.

Each event carries a deterministic event_id and a schema_version (data contract).
Optional fault injection simulates real-world problems:
  --dup-every N   re-send every Nth event (like producer retries)
  --bad-every N   add a malformed event after every Nth event (tests the DLQ)

Example:
    python streaming/producer.py --start "2025-03-14 17:00" --hours 3 --speed 30 --dup-every 100 --bad-every 500
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import requests
from confluent_kafka import Producer
from contract import SCHEMA_VERSION

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


def make_event(row, position: int) -> dict:
    """Build one event. event_id is deterministic: the same trip always gets the same id."""
    ts = row.request_datetime.isoformat()
    raw_id = (
        f"{row.hvfhs_license_num}|{ts}|{row.PULocationID}|{row.DOLocationID}|{position}"
    )
    return {
        "event_id": hashlib.sha256(raw_id.encode()).hexdigest()[:16],
        "schema_version": SCHEMA_VERSION,
        "event_time": ts,
        "company": COMPANIES.get(row.hvfhs_license_num, "Other"),
        "pu_zone_id": int(row.PULocationID),
        "do_zone_id": int(row.DOLocationID),
    }


def corrupt(event: dict, n: int) -> str:
    """Return a deliberately broken message, rotating through 5 failure types."""
    kind = n % 5
    if kind == 0:
        return "{not valid json"
    if kind == 1:
        bad = {k: v for k, v in event.items() if k != "event_time"}
    elif kind == 2:
        bad = {**event, "pu_zone_id": 999}
    elif kind == 3:
        bad = {**event, "event_time": "not-a-timestamp"}
    else:
        bad = {**event, "company": "Bolt"}
    return json.dumps(bad)


def replay(df: pd.DataFrame, args: argparse.Namespace) -> None:
    """Send each request to Kafka, paced by its real timestamp divided by speed."""
    producer = Producer(
        {"bootstrap.servers": args.bootstrap, "acks": "all", "linger.ms": 50}
    )
    counts = {"events": 0, "duplicates": 0, "bad": 0, "failed": 0}

    def on_delivery(err, _msg) -> None:
        if err is not None:
            counts["failed"] += 1

    def send(key: str, value: str) -> None:
        while True:
            try:
                producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(1)  # local queue full: let Kafka catch up
        producer.poll(0)

    first_event = df["request_datetime"].iloc[0]
    wall_start = time.monotonic()
    try:
        for position, row in enumerate(df.itertuples(index=False), start=1):
            due = (row.request_datetime - first_event).total_seconds() / args.speed
            delay = due - (time.monotonic() - wall_start)
            if delay > 0:
                time.sleep(delay)

            event = make_event(row, position)
            key, value = str(event["pu_zone_id"]), json.dumps(event)
            send(key, value)
            counts["events"] += 1

            if args.dup_every and position % args.dup_every == 0:
                send(key, value)  # same event_id again: simulated retry
                counts["duplicates"] += 1
            if args.bad_every and position % args.bad_every == 0:
                send(key, corrupt(event, counts["bad"]))
                counts["bad"] += 1

            if position % 10_000 == 0:
                print(f"  sent {position:,} | simulated time {row.request_datetime}")
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        producer.flush(30)
        total = counts["events"] + counts["duplicates"] + counts["bad"]
        print(
            f"Done in {time.monotonic() - wall_start:.0f}s: {counts['events']:,} events + "
            f"{counts['duplicates']:,} duplicates + {counts['bad']:,} bad = {total:,} messages, "
            f"failed {counts['failed']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay TLC ride requests into Kafka")
    parser.add_argument("--month", default="2025-03")
    parser.add_argument("--start", default="2025-03-14 17:00")
    parser.add_argument("--hours", type=float, default=3)
    parser.add_argument(
        "--speed", type=float, default=60, help="60 = 1 real minute per second"
    )
    parser.add_argument(
        "--dup-every", type=int, default=0, help="re-send every Nth event (0 = off)"
    )
    parser.add_argument(
        "--bad-every", type=int, default=0, help="add a bad event every N (0 = off)"
    )
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

    path = ensure_file(args.month)
    df = load_requests(path, pd.Timestamp(args.start), args.hours)
    print(f"Loaded {len(df):,} ride requests from {args.start} (+{args.hours}h)")
    replay(df, args)


if __name__ == "__main__":
    main()
