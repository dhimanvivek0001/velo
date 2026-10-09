"""Velo - monthly ingestion: NYC TLC HVFHV trips + hourly weather + permitted events.

pick_month -> download -> validate -> upload_s3 -> upload_databricks
           -> ingest_weather   (parallel)
           -> ingest_events    (parallel)
"""

from datetime import timedelta

from airflow.sdk import Param, dag, get_current_context, task

# Settings applied to every task in this DAG
default_args = {
    "owner": "vivek",
    "retries": 2,  # retry a failed task twice before giving up
    "retry_delay": timedelta(minutes=5),  # wait 5 min between retries
}


@dag(
    dag_id="tlc_monthly_ingest",
    description="One month of NYC HVFHV trips + weather + events, validated, landed in S3 + Databricks",
    schedule=None,  # manual trigger for now; monthly schedule comes in Step 1.6
    catchup=False,
    default_args=default_args,
    params={
        "year": Param(2025, type="integer", minimum=2019, maximum=2030),
        "month": Param(1, type="integer", minimum=1, maximum=12),
    },
    tags=["velo", "ingestion", "phase-1"],
)
def tlc_monthly_ingest():
    @task
    def pick_month() -> dict:
        """Read the year/month chosen when the DAG was triggered."""
        params = get_current_context()["params"]
        return {"year": int(params["year"]), "month": int(params["month"])}

    @task(execution_timeout=timedelta(hours=1))
    def download(target: dict) -> str:
        from ingestion.ingest_tlc import download as do_download

        return str(do_download(target["year"], target["month"]))

    @task
    def validate(path: str, target: dict) -> int:
        from pathlib import Path

        from ingestion.ingest_tlc import validate as do_validate

        return do_validate(Path(path), target["year"], target["month"])

    @task(execution_timeout=timedelta(hours=3))
    def upload_s3(path: str, target: dict) -> str:
        from pathlib import Path

        from ingestion.ingest_tlc import upload_to_s3

        return upload_to_s3(Path(path), target["year"], target["month"])

    @task(execution_timeout=timedelta(hours=3))
    def upload_databricks(path: str, target: dict) -> str:
        from pathlib import Path

        from ingestion.ingest_tlc import upload_to_databricks

        return upload_to_databricks(Path(path), target["year"], target["month"])

    @task(execution_timeout=timedelta(minutes=15))
    def ingest_weather(target: dict) -> str:
        """Hourly NYC weather from the Open-Meteo API. Independent of trips."""
        from ingestion import ingest_weather as weather

        year, month = target["year"], target["month"]
        df = weather.fetch(year, month)
        weather.validate(df, year, month)
        path = weather.save(df, year, month)
        weather.upload(path, year, month)
        return str(path)

    @task(execution_timeout=timedelta(minutes=15))
    def ingest_events(target: dict) -> int:
        """Permitted street events from NYC Open Data (paginated). Independent of trips."""
        from ingestion import ingest_events as events
        from ingestion.storage import upload_small_file

        year, month = target["year"], target["month"]
        df, expected = events.fetch(year, month)
        events.validate(df, expected)
        path = events.save(df, year, month)
        upload_small_file(path, events.partition_path(year, month))
        return len(df)

    # ----- Wire the tasks together -----
    target = pick_month()
    path = download(target)
    rows = validate(path, target)
    s3_key = upload_s3(path, target)
    dbx_path = upload_databricks(path, target)
    ingest_weather(target)  # parallel with trips
    ingest_events(target)  # parallel with trips

    # Uploads only start after validation passes; S3 first, then Databricks
    rows >> s3_key >> dbx_path


tlc_monthly_ingest()
