"""Live surge monitor.

Reads ride requests from Kafka, counts demand per zone in 5-minute event-time
windows, and flags zones far above their normal level for that weekday and hour.

    python streaming/surge_monitor.py
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, KafkaError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASELINE_FILE = PROJECT_ROOT / "data" / "stream" / "baseline_zone_hour.csv"
ZONES_FILE = PROJECT_ROOT / "dbt" / "velo" / "seeds" / "taxi_zone_lookup.csv"
ALERTS_FILE = PROJECT_ROOT / "data" / "stream" / "surge_windows.csv"
TOPIC = "ride_requests"
WINDOW = timedelta(minutes=5)
LATENESS = timedelta(minutes=1)  # wait this long for late events
WINDOWS_PER_HOUR = 12


def load_baseline() -> dict[tuple[int, int, int], float]:
    df = pd.read_csv(BASELINE_FILE)
    return {
        (int(r.zone_id), int(r.dow), int(r.hour_of_day)): float(r.avg_trips_per_hour)
        for r in df.itertuples(index=False)
    }


def load_zone_names() -> dict[int, str]:
    df = pd.read_csv(ZONES_FILE)
    return {
        int(r.LocationID): f"{r.Zone} ({r.Borough})" for r in df.itertuples(index=False)
    }


def window_start(ts: datetime) -> datetime:
    return ts.replace(minute=ts.minute - ts.minute % 5, second=0, microsecond=0)


def spark_dow(ts: datetime) -> int:
    """Match Spark's dayofweek(): 1 = Sunday ... 7 = Saturday."""
    return ts.isoweekday() % 7 + 1


def close_window(start, counts, baseline, names, args, writer) -> None:
    dow, hour = spark_dow(start), start.hour
    surges = []
    for zone, n in counts.items():
        expected = baseline.get((zone, dow, hour), 0.0) / WINDOWS_PER_HOUR
        ratio = n / expected if expected > 0 else None
        is_surge = ratio is not None and n >= args.min_requests and ratio >= args.ratio
        writer.writerow(
            [
                start.isoformat(),
                zone,
                names.get(zone, "?"),
                n,
                round(expected, 2),
                round(ratio, 2) if ratio else "",
                is_surge,
            ]
        )
        if is_surge:
            surges.append((ratio, zone, n, expected))

    surges.sort(reverse=True)
    total = sum(counts.values())
    end = start + WINDOW
    print(
        f"[{start:%a %H:%M}-{end:%H:%M}] {total:>5,} requests | "
        f"{len(counts):>3} zones active | {len(surges)} surging"
    )
    for ratio, zone, n, expected in surges[:5]:
        print(
            f"     SURGE  {names.get(zone, '?'):<45} {n:>4} vs normal {expected:5.1f}  ({ratio:.1f}x)"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Live surge monitor")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--group", default="velo-surge-monitor")
    parser.add_argument(
        "--ratio", type=float, default=2.0, help="surge if demand >= ratio x normal"
    )
    parser.add_argument(
        "--min-requests", type=int, default=15, help="ignore tiny zones"
    )
    parser.add_argument(
        "--idle-exit", type=int, default=15, help="stop after N quiet seconds"
    )
    args = parser.parse_args()

    baseline = load_baseline()
    names = load_zone_names()
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",  # new group: start from the oldest message
        }
    )
    consumer.subscribe([TOPIC])
    n_partitions = len(consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions)

    windows: dict[datetime, Counter] = defaultdict(Counter)
    partition_max: dict[int, datetime] = {}  # latest event time seen in each partition
    closed_until: datetime | None = None
    processed = bad = late = idle = 0

    ALERTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(ALERTS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "window_start",
                "zone_id",
                "zone_name",
                "requests",
                "expected",
                "ratio",
                "is_surge",
            ]
        )
        print(
            f"Listening on '{TOPIC}' ({n_partitions} partitions) as group '{args.group}'..."
        )
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    idle += 1
                    if idle >= args.idle_exit:
                        print("No new events, finishing.")
                        break
                    continue
                idle = 0
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"Kafka error: {msg.error()}")
                    continue

                try:
                    event = json.loads(msg.value())
                    ts = datetime.fromisoformat(event["event_time"])
                    zone = int(event["pu_zone_id"])
                except (ValueError, KeyError, TypeError):
                    bad += 1  # malformed message: skip, don't crash
                    continue

                start = window_start(ts)
                if closed_until is not None and start < closed_until:
                    late += 1  # arrived after its window was closed
                    continue
                windows[start][zone] += 1
                processed += 1

                # Per-partition watermark: Kafka only orders events WITHIN a partition,
                # so the safe watermark is the slowest partition's latest event time.
                p = msg.partition()
                partition_max[p] = max(ts, partition_max.get(p, ts))
                if len(partition_max) < n_partitions:
                    continue  # not every partition has reported yet
                watermark = min(partition_max.values())

                for s in sorted(windows):
                    if s + WINDOW + LATENESS <= watermark:
                        close_window(s, windows.pop(s), baseline, names, args, writer)
                        closed_until = s + WINDOW
                    else:
                        break
        except KeyboardInterrupt:
            print("Stopped by user.")
        finally:
            for s in sorted(windows):
                close_window(s, windows[s], baseline, names, args, writer)
            consumer.close()

    print(f"\nProcessed {processed:,} | malformed {bad} | late {late}")
    print(f"Window results saved to {ALERTS_FILE}")


if __name__ == "__main__":
    main()
