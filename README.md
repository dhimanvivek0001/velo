# Velo — Ride-Hailing Marketplace Intelligence Platform

End-to-end data platform that quantifies revenue lost to supply–demand mismatch in a ride-hailing marketplace, using ~20M real NYC TLC trips per month.

**Stack:** Airflow · Databricks (PySpark, Delta Lake, Unity Catalog) · dbt · AWS · Kafka · MLflow · Power BI

## Status
- [x] Phase 0 — Environment setup (Databricks, AWS S3/IAM, pre-commit, smoke test)
- [x] Phase 1 — Ingestion: 12 months of NYC HVFHV trips (~240M rows), hourly weather (Open-Meteo API), permitted events (NYC Open Data, paginated); orchestrated in Airflow 3
- [ ] Phase 2 — Lakehouse (bronze/silver) with PySpark + Delta
- [ ] Phase 3 — dbt modelling (gold)
