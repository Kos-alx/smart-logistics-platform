"""Синхронизация месячных партиций подготовленного Mart из PG в CH."""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="slp_sync_prices_prepared_ch",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["slp", "mart", "clickhouse"],
)
def sync_prices_prepared_ch():
    @task(retries=2, retry_delay=timedelta(minutes=1))
    def etl():
        from slp.etl.clickhouse_prices_prepared import run_clickhouse_prices_prepared_etl

        conf = get_current_context()["dag_run"].conf or {}
        return run_clickhouse_prices_prepared_etl(mode=conf.get("mode", "auto"))

    etl()


sync_prices_prepared_ch()
