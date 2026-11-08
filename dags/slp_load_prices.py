
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context

from slp.etl.prices import run_prices_etl


@dag(
    dag_id="slp_load_prices",
    schedule="@daily",
    start_date=datetime(2023, 9, 22),
    catchup=False,
    max_active_runs=1,
    tags=["slp", "etl", "prices"],
)
def load_prices():

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def extract_transform_load():
        context = get_current_context()

        interval_start = context["data_interval_start"]
        interval_end = context["data_interval_end"]

        return run_prices_etl(
            start_date=interval_start,
            end_date=interval_end,
        )

    extract_transform_load()


load_prices()
