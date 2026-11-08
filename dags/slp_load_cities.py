from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="slp_load_cities",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["slp", "etl", "reference"],
)
def load_cities_dag():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def etl():
        from slp.etl.cities import run_cities_etl

        return run_cities_etl()

    etl()


load_cities_dag()
