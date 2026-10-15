"""Daily card-transaction lakehouse pipeline (Airflow 3, TaskFlow API).

landing files ─► ingest (upsert into Iceberg) ─► rebuild daily summary ─► data quality gate
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.sdk import dag, task

DEFAULT_ARGS = {"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=5)}


@dag(
    dag_id="card_transactions_lakehouse",
    schedule="@daily",
    start_date=datetime(2026, 10, 5),
    catchup=True,
    max_active_runs=1,          # Iceberg commits are serialised per table
    default_args=DEFAULT_ARGS,
    tags=["iceberg", "polars", "payments"],
)
def card_transactions_lakehouse():
    @task
    def ingest(ds: str | None = None) -> dict:
        from lakehouse import jobs
        return jobs.ingest_day(date.fromisoformat(ds))

    @task
    def summarise(ingest_stats: dict) -> dict:
        from lakehouse import jobs
        return jobs.build_summary(date.fromisoformat(ingest_stats["day"]))

    @task
    def quality_gate(summary_stats: dict) -> dict:
        from lakehouse import jobs
        return jobs.quality_check(date.fromisoformat(summary_stats["day"]))

    quality_gate(summarise(ingest()))


card_transactions_lakehouse()
