
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.sdk import dag, task


@dag(
    dag_id="load_exchange_rates",
    description=(
        'Загрузка справочника валютных курсов в Core для пересчёта цен в рубли. Ежедневно в 09:00 по Москве.'
    ),
    schedule="0 9 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Europe/Moscow")),
    catchup=False,
    max_active_runs=1,
    tags=["slp", "etl", "reference"],
    is_paused_upon_creation=False,
)
def load_exchange_rates_dag():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def etl():
        from slp.etl.exchange_rates import run_exchange_rates_etl

        return run_exchange_rates_etl()

    etl()


load_exchange_rates_dag()
