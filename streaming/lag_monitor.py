"""Consumer lag monitor.

Lag = latest offset in Kafka - offset the consumer group has committed.
Alerts when lag is above --alert-lag AND has grown for --alert-checks checks.
Uses hysteresis: once in trouble, a group only counts as RECOVERED when lag
drops below --recover-lag (prevents alert flapping). Every reading is saved
to data/stream/lag_history.csv.

    python streaming/lag_monitor.py                         # one snapshot, all groups
    python streaming/lag_monitor.py --watch 5 --groups velo-volume-sink-v2
"""

from __future__ import annotations

import argparse
import csv
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HISTORY_FILE = PROJECT_ROOT / "data" / "stream" / "lag_history.csv"


def list_groups(admin: AdminClient) -> list[str]:
    result = admin.list_consumer_groups(request_timeout=10).result()
    return sorted(g.group_id for g in result.valid)


def group_lag(bootstrap: str, group: str, topic: str) -> list[int]:
    """Return lag per partition for one group (empty if it never consumed the topic)."""
    # Read-only: we never subscribe or commit, so the real group is not disturbed.
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False}
    )
    try:
        meta = consumer.list_topics(topic, timeout=10).topics[topic]
        partitions = [TopicPartition(topic, p) for p in sorted(meta.partitions)]
        lags = []
        for tp in consumer.committed(partitions, timeout=10):
            if tp.offset < 0:  # no committed offset for this partition
                continue
            _low, high = consumer.get_watermark_offsets(tp, timeout=10)
            lags.append(high - tp.offset)
        return lags
    finally:
        consumer.close()


def classify(total: int, growing: int, was_in_trouble: bool, args) -> str:
    if total >= args.alert_lag and growing >= args.alert_checks:
        return "ALERT: lag growing"
    if total >= args.alert_lag:
        return "HIGH"
    if was_in_trouble and total > args.recover_lag:
        return "DRAINING"  # falling, but not yet safely low (hysteresis)
    if was_in_trouble:
        return "RECOVERED"
    return "OK"


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka consumer lag monitor")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="ride_requests")
    parser.add_argument("--groups", nargs="*", help="groups to watch (default: all)")
    parser.add_argument(
        "--watch", type=float, default=0, help="seconds between checks (0 = once)"
    )
    parser.add_argument(
        "--alert-lag",
        type=int,
        default=10_000,
        help="above normal sawtooth (rate x commit interval ~ 7,000)",
    )
    parser.add_argument("--recover-lag", type=int, default=1_000)
    parser.add_argument(
        "--alert-checks",
        type=int,
        default=3,
        help="alert after lag grows this many checks in a row",
    )
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    previous: dict[str, int] = {}
    growing: Counter = Counter()
    in_trouble: dict[str, bool] = {}

    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    new_file = not HISTORY_FILE.exists()
    with open(HISTORY_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(
                [
                    "checked_at",
                    "group",
                    "topic",
                    "total_lag",
                    "partition_lags",
                    "status",
                ]
            )
        try:
            while True:
                now = datetime.now(timezone.utc)
                print(f"\n{now:%H:%M:%S} UTC | topic '{args.topic}'")
                for group in args.groups or list_groups(admin):
                    lags = group_lag(args.bootstrap, group, args.topic)
                    if not lags:
                        continue
                    total = sum(lags)
                    delta = total - previous[group] if group in previous else None
                    growing[group] = growing[group] + 1 if delta and delta > 0 else 0

                    status = classify(
                        total, growing[group], in_trouble.get(group, False), args
                    )
                    in_trouble[group] = status in (
                        "ALERT: lag growing",
                        "HIGH",
                        "DRAINING",
                    )

                    trend = f"{delta:+,}" if delta is not None else ""
                    print(
                        f"  {group:<28} lag {total:>9,} {trend:>9}  partitions {lags}  {status}"
                    )
                    writer.writerow(
                        [now.isoformat(), group, args.topic, total, lags, status]
                    )
                    previous[group] = total
                f.flush()
                if not args.watch:
                    break
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("Stopped.")


if __name__ == "__main__":
    main()
