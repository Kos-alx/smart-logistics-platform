from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task, get_current_context

from slp.etl.prices import run_prices_etl

@dag(
    dag_id="slp_load_prices",
    schedule="@daily",
    start_date=datetime(2023, 9, 22, tzinfo=timezone.utc),
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
        conf = context["dag_run"].conf or {}

        if "start_date" in conf and "end_date" in conf:
            start = datetime.fromisoformat(conf["start_date"])
            end = datetime.fromisoformat(conf["end_date"])

            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)
        else:
            start = context["data_interval_start"]
            end = context["data_interval_end"]

            if start == end:
                start = context["logical_date"]
                end = start + timedelta(days=1)

        return run_prices_etl(
            start_date=start,
            end_date=end,
        )

    extract_transform_load()


load_prices()
