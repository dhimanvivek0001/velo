# Data Profile — NYC TLC High Volume FHV (HVFHV)

**Profiled:** January 2025 file (`fhvhv_tripdata_2025-01.parquet`, 468 MB)
**Source:** NYC TLC trip record data (monthly Parquet)

## Shape
| Metric | Value |
|---|---|
| Rows | 20,405,666 |
| Columns | 25 |
| Row groups | 20 (~1M rows each) |

## Key findings

### 1. File is sorted by time — first rows are not a sample
First 500k rows = New Year's Day only (median wait 5.70 min).
Mid-month row group = 3.78 min. **NYD waits 51% longer than normal.**
→ Always sample across row groups, never `head()`.

### 2. Congestion pricing visible from 5 Jan 2025
Mid-January: 35.9% of trips carry `cbd_congestion_fee > 0`.
Zero before 5 Jan, by design. Basis for the L5 causal analysis.

### 3. Lyft does not report driver arrival
| Company | Code | Share | `on_scene_datetime` null | `originating_base_num` null |
|---|---|---|---|---|
| Uber | HV0003 | 75.3% | 0.00% | 0.00% |
| Lyft | HV0005 | 24.7% | 99.75% | 99.75% |

→ Rider wait (`pickup − request`) = both companies (primary metric).
→ Driver arrival (`on_scene − request`) = **Uber only**, labelled as such.

## Data quality rules for silver
| Rule | Sample count (NYD, 500k) | Action |
|---|---|---|
| Negative wait (pickup before request) | 4,345 (0.9%) | Flag `is_bad_wait`; exclude from wait metrics |
| Wait > 60 min | 41 | Flag `is_wait_outlier` |
| Negative fare or driver pay | 42+ | Flag `is_adjustment`; keep |
| Zero-mile trip | 145 | Flag; exclude from fare-per-mile |
| Dropoff before pickup | 0 | Keep test as safety net |
| Location ID 264/265 | present | Map to "Unknown" zone |

## Type fixes for silver
- `*_flag` columns: `'Y'/'N'` text → boolean
- Timestamps: naive, NYC local time (America/New_York) — document, handle DST
- `trip_time`: seconds (int)

## Open questions
- What do negative waits represent (clock skew vs scheduled rides)?
- Validate full-month null rates and red-flag counts in Spark (Phase 2).
