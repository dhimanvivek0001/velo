# Airflow + dbt in an isolated virtual environment (avoids dependency conflicts)
FROM apache/airflow:3.3.2-python3.11

RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-core==1.12.5" "dbt-databricks==1.12.7"
