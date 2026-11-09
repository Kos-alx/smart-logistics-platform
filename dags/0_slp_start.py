from datetime import datetime, timezone

from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)


@dag(
    dag_id="0_slp_start",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["slp", "init", "orchestration"],
)
def slp_start():

    cities = TriggerDagRunOperator(
        task_id="load_cities",
        trigger_dag_id="slp_load_cities",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    body_types = TriggerDagRunOperator(
        task_id="load_body_types",
        trigger_dag_id="slp_load_body_types",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    exchange_rates = TriggerDagRunOperator(
        task_id="load_exchange_rates",
        trigger_dag_id="slp_load_exchange_rates",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    backfill = TriggerDagRunOperator(
        task_id="backfill_prices",
        trigger_dag_id="slp_load_prices",
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
        trigger_dag_id="slp_load_price_dates",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    price_body_types = TriggerDagRunOperator(
        task_id="load_price_body_types",
        trigger_dag_id="slp_load_price_body_types",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )


    [cities, body_types, exchange_rates] >> backfill
    backfill >> [price_dates, price_body_types]


slp_start()
