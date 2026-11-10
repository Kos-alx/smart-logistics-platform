from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.sdk import dag, task


@dag(
    dag_id="load_price_body_types",
    description=(
        'Синхронизация связей сделок с типами кузовов в Core с регистрацией изменений для витрин. Ежедневно в 09:30 по Москве.'
    ),
    schedule="30 9 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Europe/Moscow")),
    catchup=False,
    max_active_runs=1,
    tags=["slp", "etl", "relations"],
    is_paused_upon_creation=False,
)
def load_price_body_types_dag():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def etl():
        from slp.etl.price_body_types import run_price_body_types_etl

        return run_price_body_types_etl()

    etl()


load_price_body_types_dag()
