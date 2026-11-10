"""Обновление сохранённых карточек маршрутов PostgreSQL и ClickHouse."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.sdk import dag, task


@dag(
    dag_id="refresh_route_cards",
    description=(
        "Обновление карточек маршрутов и коридоров рыночных ставок в PostgreSQL и ClickHouse. "
        "Ежедневно в 10:15 по Москве; обе карточки пересчитываются параллельно."
    ),
    schedule="15 10 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Europe/Moscow")),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["slp", "mart", "route_cards"],
)
def refresh_route_cards():
    @task(task_id="refresh_pg_route_card", retries=2, retry_delay=timedelta(minutes=1))
    def pg_route_card():
        from slp.etl.route_card import refresh_pg_route_card

        return refresh_pg_route_card()

    @task(task_id="refresh_ch_route_card", retries=2, retry_delay=timedelta(minutes=1))
    def ch_route_card():
        from slp.etl.route_card import refresh_ch_route_card

        return refresh_ch_route_card()

    pg_route_card()
    ch_route_card()


refresh_route_cards()
