
from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="slp_load_body_types",
    schedule=None,
    start_date=datetime(2026, 1, 1),
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
