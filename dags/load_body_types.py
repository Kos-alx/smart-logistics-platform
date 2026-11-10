
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.sdk import dag, task


@dag(
    dag_id="load_body_types",
    description=(
        'Загрузка справочника типов кузовов и их соответствий стандартным группам в Core. Ежедневно в 09:00 по Москве.'
    ),
    schedule="0 9 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Europe/Moscow")),
    catchup=False,
    max_active_runs=1,
    tags=["slp", "etl", "reference"],
    is_paused_upon_creation=False,
)
def load_body_types_dag():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def etl():
        from slp.etl.body_types import run_body_types_etl

        return run_body_types_etl()

    etl()


load_body_types_dag()
