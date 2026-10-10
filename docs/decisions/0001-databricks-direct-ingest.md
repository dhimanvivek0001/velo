# ADR 0001: Bulk trip ingestion runs inside Databricks, not via the laptop

**Date:** 2026-10-10 | **Status:** Accepted

## Context
The first design downloaded each monthly TLC file (~450 MB) to the Airflow host,
then uploaded it to S3 and the Databricks volume. Measured home uplink: ~0.2 MB/s.
One month took ~1.6 hours end to end; the 11-month backfill was projected at ~17 hours.

## Test
A HEAD request from a Databricks Free Edition notebook to the TLC CloudFront URL
succeeded, so the workspace can reach the source directly despite Free Edition's
outbound restrictions.

## Decision
Bulk trip backfills run inside Databricks (`databricks/backfill_hvfhv.py`), writing
straight to `/Volumes/velo/landing/raw/hvfhv/` with idempotent size checks.
Result: 11 months (~5.4 GB, ~220M rows) loaded in ~2 minutes.

Small API datasets (weather, events) stay on the Airflow path: they are KB-sized.

## Consequences
- S3 raw archive holds January 2025 trips only; the UC volume is the raw copy for
  the other months. Weather and events are archived in S3 for all 12 months.
- The laptop route (`ingestion/ingest_tlc.py`) remains as a tested fallback.
- Next step: Airflow should trigger the Databricks job rather than move bytes itself.
