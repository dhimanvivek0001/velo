# Velo — Ride-Hailing Marketplace Intelligence Platform

End-to-end data platform that quantifies revenue lost to supply–demand mismatch in a ride-hailing marketplace, using ~20M real NYC TLC trips per month.

**Stack:** Airflow · Databricks (PySpark, Delta Lake, Unity Catalog) · dbt · AWS · Kafka · MLflow · Power BI

## Status
- [x] Phase 0 — Environment setup (Databricks, AWS S3/IAM, pre-commit, smoke test)
- [x] Phase 1 — Ingestion: 12 months of NYC HVFHV trips (~240M rows), hourly weather (Open-Meteo API), permitted events (NYC Open Data, paginated); orchestrated in Airflow 3
- [ ] Phase 2 — Lakehouse (bronze/silver) with PySpark + Delta
- [ ] Phase 3 — dbt modelling (gold)

## Phase 3 — Gold layer with dbt ✅

dbt project (`dbt/velo`) on Databricks SQL Warehouse, building a tested star schema on top of the silver Delta tables.

![dbt lineage](docs/images/dbt_lineage.png)

| Layer | Models | Materialisation |
|---|---|---|
| Sources | `silver.hvfhv_trips`, `silver.weather_hourly`, `silver.events` | Delta tables |
| Seed | `taxi_zone_lookup` (265 TLC zones) | table |
| Staging | `stg_trips`, `stg_zones`, `stg_weather`, `stg_events` | views |
| Marts | `dim_zone`, `dim_date`, `fct_zone_hour` | tables |

**`fct_zone_hour`**: one row per pickup zone per hour. 239.6M trips → 2.2M rows (~108× smaller), with demand, median/p90 wait, revenue, weather, events, holiday and congestion-pricing flags.

**Data tests (19):** unique / not_null keys, referential integrity (trips → zones), a custom grain test (zone + hour) and a reconciliation test (sum of fact trips = source trip count).

**Finding:** the uniqueness test on `trip_id` flagged 2,988 colliding keys. Investigation showed all 6,059 rows were distinct trips (99.7% differ in dropoff time, distance and fare): same driver, request and pickup time, so likely multi-rider pickups. Fix: extend the surrogate key, rather than deleting real trips.

## Phase 4 — Real-time streaming with Kafka 🚧

Replays real ride requests as a live event stream and detects demand surges per zone in real time.

## Phase 4 — Real-time streaming with Kafka

Replays real ride requests as a live event stream and detects demand surges per zone in real time.

```
TLC parquet -> producer.py -> Kafka topic ride_requests (3 partitions) -> surge_monitor.py -> surge alerts
               (60x replay,                                               (5-min event-time windows,
                key = pickup zone)                                         per-partition watermark,
                                                                           baseline from fct_zone_hour)
```

| Component | What it does |
|---|---|
| `streaming/docker-compose.yaml` | Single-node Kafka 3.9 (KRaft mode, no ZooKeeper) |
| `streaming/producer.py` | Replays real requests in their original rhythm; only request-time fields (no future leakage); `acks=all`, backpressure handling |
| `streaming/build_baseline.py` | Exports "normal" demand per zone × weekday × hour from the dbt gold table via the Databricks SQL connector |
| `streaming/surge_monitor.py` | Tumbling 5-min windows on event time; flags zones ≥2× normal; skips malformed messages; saves every window to CSV |

**Result:** Friday 14 Mar 2025, 17:00–20:00, 128,068 events processed, 0 late, 0 malformed.

**Bug found and fixed:** the first version used a global watermark (latest event time across all partitions). Because Kafka only orders events *within* a partition, reading partition 0 first closed every window early, and **71% of events were dropped as late**. Fixed with a per-partition watermark (the minimum of each partition's latest event time), the same approach Spark and Flink use.

**Finding:** surges of 3–5× concentrated in Brooklyn's Orthodox Jewish neighbourhoods (Borough Park, Williamsburg, Midwood) and stopped at around 18:45. That date was **Purim**, and on a Friday Shabbat begins at sunset. The baseline only knows US federal holidays, which motivates cultural-calendar features in the forecasting model.
### Phase 4.4 — Kafka → Databricks (Structured Streaming into Delta)

`kafka_to_volume.py` is an at-least-once sink (offsets committed only after a successful upload), landing 10-second JSON files in a Unity Catalog volume. `databricks/06_stream_surge.py` reads them with Auto Loader as a stream, applies 5-minute event-time windows with a 10-minute watermark, writes `velo.silver.stream_zone_5min` (Delta) and exposes `velo.gold.stream_surge_alerts`.

**Stream–batch reconciliation:** streaming output = batch truth for every closed window (117,227 requests, 33 windows, 0 lost). Surges match the local Python monitor exactly.

**Lessons learned:**
- **Backlog replay ≠ live.** Sinking a pre-filled topic produced files with up to 1.5 h of event-time disorder, and Spark's *global* watermark dropped 8.6% of events. Running the sink live reduced the disorder to ≤1 s.
- **Watermark sizing = latency vs completeness.** 10 min tolerates live disorder; the cost is that results arrive 10 min late, and about 1,400 window states are held in memory.
- **Table and checkpoint are a pair.** Dropping the table without its checkpoint made the stream skip all data (the checkpoint said "already processed").
### Phase 4.5 — Data contract, dead-letter queue, idempotency (chaos-tested)

- `streaming/contract.py`: every event must have `event_id`, `schema_version`, a valid ISO `event_time`, a known company and zones 1–265. Failures return a specific reason.
- The sink routes invalid events to `ride_requests_dlq` with the error, raw payload and source partition/offset. Offsets are committed only after the upload **and** the DLQ writes succeed.
- The producer emits a deterministic `event_id`; Spark applies `dropDuplicatesWithinWatermark(event_id)` before windowing.

**Chaos test:** the producer re-sent 1 in 100 events (simulated retries) and injected 1 malformed event per 500 (5 failure types).

| | Result |
|---|---|
| Messages sent | 129,604 = 128,068 real + 1,280 duplicates + 256 malformed |
| Routed to DLQ | 256 / 256 (52 invalid JSON, 51 missing field, 51 bad zone, 51 bad timestamp, 51 unknown company) |
| Unique events after dedupe | 128,068 |
| Streaming result | 117,227 requests in 33 closed windows, **identical to the clean run**, 0 difference per window vs batch truth |
### Phase 4.6–4.7 — Observability and containerised services

**Consumer lag monitor** (`streaming/lag_monitor.py`): lag = latest offset − committed offset per partition. It alerts only when lag is above 10k **and** growing for 3 checks, with hysteresis (recovered below 1k, DRAINING in between).
- A side-by-side run with the old rule (5k threshold, no hysteresis) on the same incident: the old rule flapped HIGH↔RECOVERED 4 times; the new one went ALERT → DRAINING → RECOVERED cleanly. The threshold is set above normal lag (rate × commit interval ≈ 7k).

**Docker services** (`streaming/docker-compose.yaml`): Kafka (two listeners, `localhost:9092` for the laptop and `kafka:29092` for containers, plus a healthcheck), plus `sink` and `lag-monitor` (`restart: unless-stopped`), plus `producer` (an on-demand profile). One image, pinned library versions, credentials mounted read-only (never baked into the image).

**Incident drill:** the producer replayed 41,851 events; the sink container was stopped mid-stream. Lag climbed to 34,654 → ALERT; the sink was restarted → one catch-up upload of **exactly 34,654** events → RECOVERED. No loss, no duplicates.

**Bug found:** the Kafka volume was mounted at `/var/lib/kafka/data`, but the image writes to `/tmp/kraft-combined-logs` by default, so topics vanished when the container was recreated. Fixed with `KAFKA_LOG_DIRS` and verified with `--force-recreate`.
