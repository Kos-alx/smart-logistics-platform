from datetime import UTC, datetime

from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import dag, task


@dag(
    dag_id="0_slp_start",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
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


    prices_prepared = TriggerDagRunOperator(
        task_id="load_prices_prepared",
        trigger_dag_id="slp_load_prices_prepared",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    clickhouse = TriggerDagRunOperator(
        task_id="sync_prices_prepared_ch",
        trigger_dag_id="slp_sync_prices_prepared_ch",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=False,
    )

    @task(task_id="refresh_pg_route_card", retries=2)
    def pg_route_card():
        from slp.etl.route_card import refresh_pg_route_card

        return refresh_pg_route_card()

    @task(task_id="refresh_ch_route_card", retries=2)
    def ch_route_card():
        from slp.etl.route_card import refresh_ch_route_card

        return refresh_ch_route_card()

    pg_card = pg_route_card()
    ch_card = ch_route_card()

    [cities, body_types, exchange_rates] >> backfill
    backfill >> [price_dates, price_body_types]
    [price_dates, price_body_types] >> prices_prepared
    prices_prepared >> clickhouse
    prices_prepared >> pg_card
    clickhouse >> ch_card


slp_start()
