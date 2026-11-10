from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.sdk import dag, task


@dag(
    dag_id="load_cities",
    description=(
        'Загрузка справочника городов в Core с устранением дублей по gc_id: сохраняется строка с минимальным id. Ежедневно в 09:00 по Москве.'
    ),
    schedule="0 9 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Europe/Moscow")),
    catchup=False,
    max_active_runs=1,
    tags=["slp", "etl", "reference"],
    is_paused_upon_creation=False,
)
def load_cities_dag():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def etl():
        from slp.etl.cities import run_cities_etl

        return run_cities_etl()

    etl()


load_cities_dag()
