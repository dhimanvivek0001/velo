"""Data contract for ride_request events (schema_version 1).

Every consumer validates events against this contract. Invalid events are
not dropped silently: the caller routes them to the dead-letter queue.
"""

from __future__ import annotations

import json
from datetime import datetime

SCHEMA_VERSION = 1
FIELDS = {
    "event_id": str,
    "schema_version": int,
    "event_time": str,
    "company": str,
    "pu_zone_id": int,
    "do_zone_id": int,
}
COMPANIES = {"Uber", "Lyft", "Other"}
MIN_ZONE, MAX_ZONE = 1, 265


def validate(raw: bytes | None) -> tuple[dict | None, str | None]:
    """Return (event, None) if the message is valid, else (None, reason)."""
    try:
        event = json.loads(raw)
    except (TypeError, ValueError):
        return None, "invalid_json"
    if not isinstance(event, dict):
        return None, "not_an_object"

    for field, field_type in FIELDS.items():
        if field not in event:
            return None, f"missing_{field}"
        value = event[field]
        if isinstance(value, bool) or not isinstance(value, field_type):
            return None, f"wrong_type_{field}"

    if event["schema_version"] != SCHEMA_VERSION:
        return None, "unsupported_schema_version"
    try:
        datetime.fromisoformat(event["event_time"])
    except ValueError:
        return None, "bad_event_time"
    if event["company"] not in COMPANIES:
        return None, "unknown_company"
    for field in ("pu_zone_id", "do_zone_id"):
        if not MIN_ZONE <= event[field] <= MAX_ZONE:
            return None, f"zone_out_of_range_{field}"
    return event, None
