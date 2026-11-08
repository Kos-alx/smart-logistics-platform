from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="slp_load_price_body_types",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
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
