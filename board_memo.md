# Board memo: Velo marketplace intelligence, findings and decisions

**To:** Executive team / Board · **From:** Marketplace Analytics · **Basis:** 360 million NYC Uber and Lyft trips, Jan 2024 – Jun 2025 (NYC TLC public data)

---

## Bottom line

1. **Congestion pricing cut ride-hail pickups inside Manhattan's charging zone by 6.8%**, about 12,300 trips a day, and the effect is **still growing** (−5% in January, −9% by June).
2. **Service is unequal across the city.** Staten Island riders have the longest typical wait (5.1 minutes, about 30% longer than Manhattan); Newark Airport is a 5× wait outlier.
3. **A day-ahead demand forecast is ready for pilot.** It is 9% more accurate than the current rule-of-thumb and cuts holiday error by a third.
4. **The data foundation is production-grade.** Every trip is reconciled, tested daily and served through monitored, version-controlled pipelines. A guarded AI copilot now answers routine data questions in seconds.

---

## 1. Congestion pricing (since 5 January 2025)

| Measure | Result |
|---|---|
| Effect on pickups inside the zone | **−6.8%** (95% confidence 4.9% to 8.7%) |
| Trips lost | ~12,300 per day; ~2.1 million in the first six months |
| Trend | −5.2% (Jan) → −9.1% (Jun) vs the same month a year earlier |
| Share of 2025 trips paying the fee | 33.9% |

The estimate compares the zone with the rest of the city, controls for seasonality and passes placebo and robustness checks, so it is a **causal effect, not a before/after trend**. Because the effect is still deepening, early industry estimates understate it.

**Decision requested:** approve an **A/B pilot of absorbing part of the fee at off-peak hours** in the zone, measured against the −6.8% baseline, alongside driver rebalancing towards the outer boroughs, where demand kept growing.

## 2. Service quality

- **Staten Island:** typical wait 5.1 minutes, the longest of any borough; Manhattan has the shortest.
- **Airports:** Newark waits are a 5× outlier; JFK and LaGuardia carry 3× the hourly demand of a typical Manhattan zone.
- **Peak:** demand peaks at 18:00 citywide.

**Decision requested:** fund a **targeted supply-incentive pilot for Staten Island**, with wait time as the success metric, on the dashboard already in place.

## 3. Demand forecasting

| | Error (WAPE) |
|---|---|
| Current rule (4-week same-hour average) | 13.6% |
| **New model** | **12.3%** (9% less error) |
| On public holidays | 31% → 21% |

- **Measured value:** about **8,200 fewer trips mis-forecast per day**. *If* a driver completes about 2 trips an hour, that is roughly 4,100 driver-hours a day positioned better (an assumption, to be validated in the pilot).
- **Weak spots:** airports (demand follows flight schedules we don't yet have) and quiet zones.
- **Already in place:** the model is registered behind a written promotion gate, served through a tested API, checked by automated tests on every code change, and monitored daily for accuracy, data drift and broken feeds.

**Decision requested:** approve a **driver-positioning pilot using the forecast** in two boroughs, and the purchase of **flight-schedule data** for the airport zones.

## 4. Data and AI capability

- **Trust:** 360,465,639 raw trips reconciled exactly at every stage; automated quality tests run daily; every model input is time-stamped so forecasts cannot use information from the future.
- **AI copilot:** answers plain-English questions from approved tables only, read-only, with every query shown and logged. **42 out of 42** test answers correct across repeated runs, including refusing out-of-scope or unsafe requests.

**Decision requested:** roll the copilot out to the **analyst team (read-only)**, with a fresh set of test questions as the gate for wider access.

## 5. Risks and limitations

- **Ride-hail only:** taxis, private cars and transit are not covered, so the findings say nothing about total traffic.
- **Weather gap:** weather history is missing for Jan–Jun 2024, which weakens weather-related forecasting; a backfill is planned.
- **Copilot accuracy** was measured on the questions used to tune it; real-world accuracy will be lower until a held-out test set confirms it.
- **The pricing effect** covers the first six months only; the long-run effect may be larger.

---

*Methods and code: github.com/dhimanvivek0001/velo (Phase 8 causal analysis, Phase 7 forecasting, Phases 6 and 9 feature store and MLOps, Phase 11 copilot). Detailed policy memo: `docs/congestion_pricing_memo.md`.*
