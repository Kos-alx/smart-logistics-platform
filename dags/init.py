from datetime import UTC, datetime

from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import dag


@dag(
    dag_id="init",
    description=(
        "Инициализация проекта: загрузка Core, построение витрин PostgreSQL и ClickHouse, "
        "обучение модели и сохранение прогнозов. Ручной запуск без расписания."
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["slp", "init", "orchestration"],
)
def init():

    cities = TriggerDagRunOperator(
        task_id="load_cities",
        trigger_dag_id="load_cities",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    body_types = TriggerDagRunOperator(
        task_id="load_body_types",
        trigger_dag_id="load_body_types",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    exchange_rates = TriggerDagRunOperator(
        task_id="load_exchange_rates",
        trigger_dag_id="load_exchange_rates",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    backfill = TriggerDagRunOperator(
        task_id="backfill_prices",
        trigger_dag_id="load_prices",
        conf={
            "start_date": "2023-09-22T00:00:00+00:00",
            "end_date": "2026-10-27T00:00:00+00:00",
        },
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    price_dates = TriggerDagRunOperator(
        task_id="load_price_dates",
        trigger_dag_id="load_price_dates",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    price_body_types = TriggerDagRunOperator(
        task_id="load_price_body_types",
        trigger_dag_id="load_price_body_types",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )


    prices_prepared = TriggerDagRunOperator(
        task_id="load_prices_prepared",
        trigger_dag_id="load_prices_prepared",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    clickhouse = TriggerDagRunOperator(
        task_id="prices_prepared_ch_cync",
        trigger_dag_id="prices_prepared_ch_cync",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    ml = TriggerDagRunOperator(
        task_id="train_and_score_ml",
        trigger_dag_id="slp_ml_prices",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    route_cards = TriggerDagRunOperator(
        task_id="refresh_route_cards",
        trigger_dag_id="refresh_route_cards",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    [cities, body_types, exchange_rates] >> backfill
    backfill >> [price_dates, price_body_types]
    [price_dates, price_body_types] >> prices_prepared
    [price_dates, price_body_types] >> ml
    prices_prepared >> clickhouse
    clickhouse >> route_cards


init()
