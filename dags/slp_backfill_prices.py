from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from slp.etl.prices import run_prices_etl


@dag(
    dag_id="slp_backfill_prices",
    schedule=None,
    start_date=datetime(2023, 9, 22, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["slp", "etl", "prices", "backfill"],
)
def backfill_prices():

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def extract_transform_load():
        return run_prices_etl(
            start_date=datetime(2023, 9, 22, tzinfo=timezone.utc),
            end_date=datetime(2026, 10, 27, tzinfo=timezone.utc),
        )

    extract_transform_load()


backfill_prices()
