"""Синхронизация месячных партиций подготовленного Mart из PG в CH."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="prices_prepared_ch_cync",
    description=(
        'Перенос подготовленных данных из PostgreSQL в ClickHouse с атомарной заменой затронутых месячных партиций. Ежедневно в 10:00 по Москве.'
    ),
    schedule="0 10 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Europe/Moscow")),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["slp", "mart", "clickhouse"],
)
def prices_prepared_ch_cync():
    @task(retries=2, retry_delay=timedelta(minutes=1))
    def etl():
        from slp.etl.clickhouse_prices_prepared import run_clickhouse_prices_prepared_etl

        conf = get_current_context()["dag_run"].conf or {}
        return run_clickhouse_prices_prepared_etl(mode=conf.get("mode", "auto"))

    etl()


prices_prepared_ch_cync()
