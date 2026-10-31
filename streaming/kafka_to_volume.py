"""Kafka -> Databricks volume sink (at-least-once, live, with dead-letter queue).

Validates every message against the data contract. Valid events are uploaded
as small JSON-lines files to the Unity Catalog volume; invalid ones go to the
ride_requests_dlq topic with the failure reason. Offsets are committed only
after the upload AND the DLQ writes succeed, so nothing is ever lost.

Live run: start this first, wait for "Ready", then start the producer.
    python streaming/kafka_to_volume.py
"""

from __future__ import annotations

import argparse
import io
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer
from contract import validate
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
TOPIC = "ride_requests"
DLQ_TOPIC = "ride_requests_dlq"
VOLUME_ROOT = "/Volumes/velo/landing/raw/stream"


def upload_batch(
    ws: WorkspaceClient, volume_dir: str, lines: list[str], batch_no: int
) -> str:
    """Write one JSON-lines file to the volume and return its path."""
    name = f"batch_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{batch_no:05d}.jsonl"
    path = f"{volume_dir}/{name}"
    data = ("\n".join(lines) + "\n").encode("utf-8")
    ws.files.upload(path, io.BytesIO(data), overwrite=True)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kafka -> Databricks volume sink with DLQ"
    )
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--group", default="velo-volume-sink-v2")
    parser.add_argument(
        "--dir", default="ride_requests_v2", help="folder inside the volume"
    )
    parser.add_argument(
        "--offset-reset", default="latest", choices=["latest", "earliest"]
    )
    parser.add_argument("--flush-seconds", type=float, default=10)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--idle-exit", type=int, default=20)
    args = parser.parse_args()

    volume_dir = f"{VOLUME_ROOT}/{args.dir}"
    ws = WorkspaceClient(profile=os.environ.get("DATABRICKS_PROFILE", "velo"))
    ws.files.create_directory(volume_dir)

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": args.offset_reset,  # latest = only NEW events (live)
            "enable.auto.commit": False,  # we commit manually, only after upload
        }
    )
    consumer.subscribe(
        [TOPIC],
        on_assign=lambda _c, parts: print(
            f"Ready: listening on {len(parts)} partitions. Start the producer now."
        ),
    )
    dlq = Producer({"bootstrap.servers": args.bootstrap, "acks": "all"})

    buffer: list[str] = []
    reasons: Counter = Counter()
    batch_no = total = idle = 0
    last_flush = time.monotonic()

    def send_to_dlq(msg, reason: str) -> None:
        value = msg.value()
        record = {
            "error": reason,
            "raw": value.decode("utf-8", errors="replace") if value else None,
            "source_partition": msg.partition(),
            "source_offset": msg.offset(),
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        dlq.produce(DLQ_TOPIC, value=json.dumps(record))
        dlq.poll(0)
        reasons[reason] += 1

    def flush() -> None:
        nonlocal buffer, batch_no, total, last_flush
        last_flush = time.monotonic()
        if not buffer:
            return
        batch_no += 1
        path = upload_batch(ws, volume_dir, buffer, batch_no)
        dlq.flush(10)  # make sure dead letters are stored too
        consumer.commit(asynchronous=False)  # both succeeded -> safe to commit
        total += len(buffer)
        print(
            f"  uploaded {len(buffer):,} events -> {path.rsplit('/', 1)[-1]}"
            f" | dead letters so far: {sum(reasons.values())}"
        )
        buffer = []

    print(
        f"Sinking '{TOPIC}' to {volume_dir} (flush every {args.flush_seconds:.0f}s) ..."
    )
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {msg.error()}")
            else:
                idle = 0
                event, reason = validate(msg.value())
                if reason:
                    send_to_dlq(msg, reason)
                else:
                    buffer.append(json.dumps(event))

            due = time.monotonic() - last_flush >= args.flush_seconds
            if len(buffer) >= args.batch_size or (buffer and due):
                flush()
            if args.idle_exit and idle >= args.idle_exit and (total or buffer):
                break  # stream went quiet after receiving data
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        flush()
        dlq.flush(10)
        consumer.close()

    print(
        f"Done: {total:,} valid events in {batch_no} files, {sum(reasons.values())} dead letters"
    )
    for reason, n in reasons.most_common():
        print(f"   DLQ {reason:<35} {n}")


if __name__ == "__main__":
    main()
