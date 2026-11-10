"""Контракты признаков, временного разбиения и сохраняемой ML-модели."""

import numpy as np
import pandas as pd
import pytest

from slp.ml.features import FEATURE_COLUMNS, make_features, split_dataset
from slp.ml.training import predict_artifact, train_artifact


def sample_data():
    """Сделки с границами окон и намеренно некорректными историческими строками."""
    dates = [
        "2026-07-25",
        "2026-07-26",
        "2026-10-04",
        "2026-10-05",
        "2026-10-26",
        "2026-10-27",
        "2027-01-02",
        "2026-09-01",
        "2026-09-02",
        "2026-09-03",
    ]
    rows = []
    for i, day in enumerate(dates):
        rows.append(
            {
                "price_id": i + 1,
                "loading_date": pd.Timestamp(day),
                "loading_city_gcid": 1,
                "unloading_city_gcid": 2,
                "origin_region": "Регион А",
                "destination_region": "Регион Б",
                "origin_latitude": 55.0,
                "origin_longitude": 37.0,
                "destination_latitude": 56.0,
                "destination_longitude": 40.0,
                "distance": 100.0 + i,
                "total_weight": 10.0,
                "total_volume": 80.0,
                "weight_category": "FTL",
                "volume_category": "FTL",
                "body_group_ids": [4, 56],
                "is_fact": True,
                "price_type": 3,
                "complex": False,
                "outlier_mark": False,
                "price_rub": 10000.0 + i,
            }
        )
    frame = pd.DataFrame(rows)
    frame.loc[7, "outlier_mark"] = True
    frame.loc[8, "is_fact"] = False
    frame.loc[9, "complex"] = True
    frame.loc[5, ["price_rub", "is_fact", "outlier_mark"]] = [np.nan, False, True]
    return frame


def test_split_boundaries_and_future_without_target():
    parts, report = split_dataset(sample_data())
    assert parts["train"].price_id.tolist() == [2, 3]
    assert parts["test"].price_id.tolist() == [4, 5]
    assert parts["future"].price_id.tolist() == [6, 7]
    assert parts["future"].price_rub.isna().all()
    assert report["train"]["rows"] == 2
    bad = sample_data()
    bad.loc[1, "price_id"] = bad.loc[0, "price_id"]
    with pytest.raises(ValueError, match="price_id"):
        split_dataset(bad)


def test_features_are_independent_of_target_and_support_unknown_categories():
    frame = sample_data()
    expected = make_features(frame)
    frame["price_rub"] = -100000.0
    frame["outlier_mark"] = True
    pd.testing.assert_frame_equal(make_features(frame), expected)
    assert list(expected.columns) == FEATURE_COLUMNS
    assert expected.body_tent.eq(1).all() and expected.body_reefer.eq(1).all()
    frame.loc[0, "origin_region"] = None
    frame.loc[1, "destination_region"] = "Новый регион"
    result = make_features(frame)
    assert result.origin_region.iloc[0] == "Не указан"
    assert result.destination_region.iloc[1] == "Новый регион"
    assert "price_id" not in result and "price_rub" not in result


def training_data():
    frame = pd.concat([sample_data().iloc[[1]].copy() for _ in range(70)], ignore_index=True)
    frame["price_id"] = np.arange(1, 71)
    frame["loading_date"] = pd.date_range("2026-07-26", periods=70)
    frame["distance"] = 100.0 + np.arange(70) * 20
    frame["price_rub"] = frame.distance * 50 + 5000
    return pd.concat(
        [frame, sample_data().iloc[[3, 4, 5, 6]].assign(price_id=[101, 102, 103, 104])],
        ignore_index=True,
    )


def test_artifact_roundtrip_reuse_and_future_nulls(tmp_path):
    frame = training_data()
    manifest = train_artifact(frame, tmp_path, max_iterations=8, early_stopping_rounds=3)
    predictions = predict_artifact(tmp_path, manifest["model_version"])
    assert len(predictions) == 4
    future = predictions.loc[predictions.prediction_set == "future"]
    assert future[["actual_price_rub", "abs_error_rub", "ape_pct"]].isna().all().all()
    assert np.isfinite(predictions.predicted_price_rub).all()
    assert predictions.price_id.is_unique
    repeated = train_artifact(frame, tmp_path, max_iterations=8, early_stopping_rounds=3)
    assert repeated["model_version"] == manifest["model_version"]
    pd.testing.assert_frame_equal(
        predictions, predict_artifact(tmp_path, manifest["model_version"])
    )
    changed = frame.copy()
    changed.loc[0, "price_rub"] += 100
    assert (
        train_artifact(changed, tmp_path, max_iterations=8, early_stopping_rounds=3)[
            "model_version"
        ]
        != manifest["model_version"]
    )
    model_path = tmp_path / manifest["model_version"] / "model.cbm"
    model_path.write_bytes(b"broken")
    with pytest.raises(ValueError, match="контроль"):
        predict_artifact(tmp_path, manifest["model_version"])


@pytest.mark.skipif(
    __import__("os").getenv("SLP_RUN_DB_TESTS") != "1",
    reason="Нужна PostgreSQL: SLP_RUN_DB_TESTS=1",
)
def test_prediction_publication_is_atomic_and_idempotent(tmp_path):
    """Повторная публикация не дублирует данные, ошибка после DELETE не теряет их."""
    from slp.db import get_connection
    from slp.ml.pipeline import ensure_tables, publish_predictions

    manifest = train_artifact(training_data(), tmp_path, max_iterations=8, early_stopping_rounds=3)
    predictions = predict_artifact(tmp_path, manifest["model_version"])
    with get_connection("SLP_CORE_DB_HOST") as conn:
        try:
            ensure_tables(conn)
            publish_predictions(conn, manifest, predictions)
            publish_predictions(conn, manifest, predictions)
            rows = conn.execute(
                """
                SELECT count(*), count(DISTINCT price_id),
                       count(*) FILTER (WHERE prediction_set = 'future'
                           AND actual_price_rub IS NULL AND abs_error_rub IS NULL AND ape_pct IS NULL)
                FROM mart.ml_price_predictions WHERE model_version = %s
            """,
                (manifest["model_version"],),
            ).fetchone()
            assert rows == (4, 4, 2)
            invalid = predictions.copy()
            invalid.loc[invalid.index[0], "prediction_set"] = "invalid"
            with pytest.raises(Exception, match="check constraint"):
                publish_predictions(conn, manifest, invalid)
            assert (
                conn.execute(
                    "SELECT count(*) FROM mart.ml_price_predictions WHERE model_version = %s",
                    (manifest["model_version"],),
                ).fetchone()[0]
                == 4
            )
        finally:
            conn.rollback()


def test_scoring_rejects_changed_feature_code(tmp_path):
    """Старая модель не должна молча получать признаки, рассчитанные другим кодом."""
    import json

    manifest = train_artifact(training_data(), tmp_path, max_iterations=8, early_stopping_rounds=3)
    path = tmp_path / manifest["model_version"] / "manifest.json"
    manifest["source"]["features.py"] = "outdated-code"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="код признаков"):
        predict_artifact(tmp_path, manifest["model_version"])


@pytest.mark.skipif(
    __import__("os").getenv("SLP_RUN_DB_TESTS") != "1",
    reason="Нужна PostgreSQL: SLP_RUN_DB_TESTS=1",
)
def test_existing_admin_initialized_tables_are_usable_by_app():
    """Чистая инициализация создаёт таблицы от администратора, DAG работает от приложения."""
    import os

    import psycopg
    from psycopg import sql

    from slp.ml.pipeline import ensure_tables

    with psycopg.connect(
        host=os.environ["SLP_CORE_DB_HOST"],
        port=os.environ["SLP_DB_PORT"],
        dbname=os.environ["SLP_DB_NAME"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ) as conn:
        try:
            ensure_tables(conn)
            for table in ("ml_models", "ml_price_predictions"):
                conn.execute(
                    sql.SQL("ALTER TABLE mart.{} OWNER TO {}").format(
                        sql.Identifier(table), sql.Identifier(os.environ["POSTGRES_USER"])
                    )
                )
                conn.execute(
                    sql.SQL("GRANT ALL ON mart.{} TO {}").format(
                        sql.Identifier(table), sql.Identifier(os.environ["SLP_DB_USER"])
                    )
                )
            conn.execute(
                sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(os.environ["SLP_DB_USER"]))
            )
            ensure_tables(conn)
        finally:
            conn.rollback()
