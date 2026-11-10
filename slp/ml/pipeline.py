"""Обучение и идемпотентный batch inference для Airflow и ручного запуска."""

import argparse
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
from psycopg.types.json import Jsonb

from slp.db import get_connection
from slp.ml.data import SQL_ROOT, load_ml_dataset
from slp.ml.training import load_manifest, predict_artifact, train_artifact

logger = logging.getLogger(__name__)


def artifact_root() -> Path:
    """Общий каталог модели и снимков; путь переопределяется окружением."""
    return Path(os.getenv("SLP_ML_ARTIFACT_DIR", str(SQL_ROOT.parent / "artifacts/ml")))


def ensure_tables(conn) -> None:
    """Для существующего volume использовать тот же DDL, что при чистой инициализации."""
    existing = conn.execute(
        "SELECT to_regclass('mart.ml_models'), to_regclass('mart.ml_price_predictions')"
    ).fetchone()
    if all(existing):
        return
    conn.execute((SQL_ROOT / "mart/03_ml_prices.sql").read_text())


def register_model(conn, manifest: dict) -> None:
    """Сохранить неизменяемую версию модели и её метаданные в той же транзакции."""
    conn.execute(
        """
        INSERT INTO mart.ml_models (
            model_version, model_name, as_of, artifact_path, model_params,
            feature_columns, metrics, manifest, trained_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (model_version) DO NOTHING
    """,
        (
            manifest["model_version"],
            manifest["model_name"],
            manifest["as_of"],
            f"{manifest['model_version']}/model.cbm",
            Jsonb({**manifest["params"], "iterations": manifest["iterations"]}),
            Jsonb(manifest["features"]),
            Jsonb(manifest["metrics"]),
            Jsonb(manifest),
            manifest["trained_at"],
        ),
    )


def publish_predictions(conn, manifest: dict, predictions: pd.DataFrame) -> None:
    """Атомарно заменить прогнозы версии; повторный запуск не создаёт дублей."""
    version = manifest["model_version"]
    columns = [
        "price_id",
        "loading_date",
        "loading_city_gcid",
        "unloading_city_gcid",
        "body_group_ids",
        "prediction_set",
        "actual_price_rub",
        "predicted_price_rub",
        "abs_error_rub",
        "ape_pct",
        "model_version",
        "model_name",
        "as_of",
    ]
    if not predictions.price_id.is_unique or not predictions.model_version.eq(version).all():
        raise ValueError("Неуникальные сделки или смешанные версии прогноза")
    if not np.isfinite(predictions.predicted_price_rub).all():
        raise ValueError("Неконечные прогнозы")
    # Вложенная транзакция обеспечивает откат DELETE даже при обработанном исключении.
    with conn.transaction():
        register_model(conn, manifest)
        conn.execute("DELETE FROM mart.ml_price_predictions WHERE model_version = %s", (version,))
        with conn.cursor() as cursor:
            for offset in range(0, len(predictions), 5000):
                batch = predictions.iloc[offset : offset + 5000][columns].copy()
                for column in ("actual_price_rub", "abs_error_rub", "ape_pct"):
                    batch[column] = batch[column].astype(object).where(batch[column].notna(), None)
                batch["loading_date"] = pd.to_datetime(batch.loading_date).dt.date
                cursor.executemany(
                    """
                    INSERT INTO mart.ml_price_predictions (
                        price_id, loading_date, loading_city_gcid, unloading_city_gcid,
                        body_group_ids, prediction_set, actual_price_rub, predicted_price_rub,
                        abs_error_rub, ape_pct, model_version, model_name, as_of
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    batch.itertuples(index=False, name=None),
                )


def train_pipeline() -> dict:
    """Обучить или повторно использовать проверенную модель того же снимка."""
    with get_connection("SLP_CORE_DB_HOST") as conn:
        conn.execute("SELECT pg_advisory_xact_lock(7341004)")
        ensure_tables(conn)
        manifest = train_artifact(load_ml_dataset(), artifact_root())
        register_model(conn, manifest)
    logger.info("Версия ML=%s; метрики=%s", manifest["model_version"], manifest["metrics"])
    return {"model_version": manifest["model_version"], "metrics": manifest["metrics"]}


def score_pipeline(model_version: str) -> dict:
    """Прогнозировать сохранённый снимок и опубликовать результат в PG."""
    with get_connection("SLP_CORE_DB_HOST") as conn:
        conn.execute("SELECT pg_advisory_xact_lock(7341004)")
        ensure_tables(conn)
        manifest = load_manifest(artifact_root(), model_version)
        predictions = predict_artifact(artifact_root(), model_version)
        publish_predictions(conn, manifest, predictions)
    result = {
        "model_version": model_version,
        "rows": len(predictions),
        "test_rows": int(predictions.prediction_set.eq("test").sum()),
        "future_rows": int(predictions.prediction_set.eq("future").sum()),
    }
    logger.info("Batch inference завершён: %s", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Обучение и batch inference тарифов")
    parser.add_argument("stage", choices=["train", "score", "all"], default="all", nargs="?")
    parser.add_argument("--model-version")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.stage in ("train", "all"):
        trained = train_pipeline()
        if args.stage == "all":
            score_pipeline(trained["model_version"])
    elif args.model_version:
        score_pipeline(args.model_version)
    else:
        parser.error("Для score требуется --model-version")
