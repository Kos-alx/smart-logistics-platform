"""Обучение тарифной модели и batch inference на сохранённом снимке Core."""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="slp_ml_prices",
    description=(
        'Обучение CatBoost на данных Core, сохранение модели и запись прогнозов для test и будущих заявок в PostgreSQL.'
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["slp", "ml"],
)
def ml_prices_dag():
    @task(retries=1, retry_delay=timedelta(minutes=1))
    def train():
        from slp.ml.pipeline import train_pipeline

        return train_pipeline()

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def score(trained: dict):
        from slp.ml.pipeline import score_pipeline

        return score_pipeline(trained["model_version"])

    score(train())


ml_prices_dag()
