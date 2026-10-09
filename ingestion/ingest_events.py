"""Ingest one month of NYC permitted street events from NYC Open Data (Socrata API).

count -> paginated fetch -> validate (row count matches) -> save Parquet -> upload
"""

import argparse
import calendar
import logging
import math

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.storage import PROJECT_ROOT, upload_small_file

# ---------- Settings ----------
API_URL = "https://data.cityofnewyork.us/resource/bkfu-528j.json"  # historical permitted events
PAGE_SIZE = 1000  # rows per request
LOCAL_DIR = PROJECT_ROOT / "data" / "raw" / "events"
REQUIRED_COLUMNS = {
    "event_id",
    "event_name",
    "start_date_time",
    "end_date_time",
    "event_type",
    "event_borough",
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("ingest_events")


def file_name(year: int, month: int) -> str:
    return f"events_nyc_{year}-{month:02d}.parquet"


def partition_path(year: int, month: int) -> str:
    return f"events/year={year}/month={month:02d}/{file_name(year, month)}"


def make_session() -> requests.Session:
    """Retry on rate limits (429) and server errors, with exponential backoff."""
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def where_clause(year: int, month: int) -> str:
    """SoQL filter: events that START inside the month."""
    last_day = calendar.monthrange(year, month)[1]
    return (
        f"start_date_time between '{year}-{month:02d}-01T00:00:00' "
        f"and '{year}-{month:02d}-{last_day:02d}T23:59:59'"
    )


# ---------- Step 1: Count, then fetch page by page ----------
def count_events(session: requests.Session, where: str) -> int:
    response = session.get(
        API_URL, params={"$select": "count(*)", "$where": where}, timeout=60
    )
    response.raise_for_status()
    return int(response.json()[0]["count"])


def fetch(year: int, month: int) -> tuple[pd.DataFrame, int]:
    session = make_session()
    where = where_clause(year, month)

    expected = count_events(session, where)
    log.info(
        "API reports %d events for %d-%02d -> %d pages of %d",
        expected,
        year,
        month,
        math.ceil(expected / PAGE_SIZE),
        PAGE_SIZE,
    )

    rows: list[dict] = []
    offset, page = 0, 1
    while True:
        params = {
            "$where": where,
            "$order": ":id",  # stable sort order, so pages never overlap or skip rows
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        response = session.get(API_URL, params=params, timeout=60)
        response.raise_for_status()
        batch = response.json()
        log.info("Page %d: %d rows (offset %d)", page, len(batch), offset)
        rows.extend(batch)

        if (
            len(batch) < PAGE_SIZE
        ):  # a short (or empty) page means we've reached the end
            break
        offset += PAGE_SIZE
        page += 1

    df = pd.DataFrame(rows)
    for col in ("start_date_time", "end_date_time"):
        df[col] = pd.to_datetime(df[col])
    return df, expected


# ---------- Step 2: Validate ----------
def validate(df: pd.DataFrame, expected: int) -> None:
    # Completeness: did pagination return every row the API said exists?
    if len(df) != expected:
        raise ValueError(f"Expected {expected} rows, fetched {len(df)}")

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    unique_events = df["event_id"].nunique()
    log.info(
        "Validation passed: %d rows, %d unique event_ids (one event can cover several streets)",
        len(df),
        unique_events,
    )
    log.info("Rows by borough: %s", df["event_borough"].value_counts().to_dict())


# ---------- Step 3: Save locally ----------
def save(df: pd.DataFrame, year: int, month: int) -> "Path":  # noqa: F821
    dest = LOCAL_DIR / file_name(year, month)
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, index=False)
    log.info("Saved %s (%.1f KB)", dest, dest.stat().st_size / 1024)
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest one month of NYC permitted events"
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    df, expected = fetch(args.year, args.month)
    validate(df, expected)
    path = save(df, args.year, args.month)
    upload_small_file(path, partition_path(args.year, args.month))
    log.info("Done: events %d-%02d", args.year, args.month)


if __name__ == "__main__":
    main()
