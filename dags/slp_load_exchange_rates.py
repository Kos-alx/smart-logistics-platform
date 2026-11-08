
from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="slp_load_exchange_rates",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["slp", "etl", "reference"],
)
def load_exchange_rates_dag():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def etl():
        from slp.etl.exchange_rates import run_exchange_rates_etl

        return run_exchange_rates_etl()

    etl()


load_exchange_rates_dag()
