"""Обучение CatBoost, проверяемые артефакты и прогноз по сохранённому снимку."""

import gzip
import hashlib
import importlib.metadata
import json
import logging
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    r2_score,
    root_mean_squared_error,
)

from slp.ml.features import (
    AS_OF,
    CATEGORICAL_FEATURES,
    FEATURE_COLUMNS,
    TARGET,
    TEST_START,
    TRAIN_START,
    VALIDATION_START,
    make_features,
    split_dataset,
)

logger = logging.getLogger(__name__)
MODEL_PARAMS = {
    "loss_function": "RMSE",
    "depth": 6,
    "learning_rate": 0.05,
    "random_seed": 42,
    "thread_count": 4,
    "task_type": "CPU",
    "allow_writing_files": False,
}


def metrics(actual, predicted) -> dict:
    """Метрики на исходной рублёвой шкале; MAPE выражена в процентах."""
    return {
        "rows": len(actual),
        "R2": float(r2_score(actual, predicted)),
        "RMSE_RUB": float(root_mean_squared_error(actual, predicted)),
        "MAE_RUB": float(mean_absolute_error(actual, predicted)),
        "MAPE_pct": float(100 * mean_absolute_percentage_error(actual, predicted)),
    }


def digest(path: Path) -> str:
    """Контрольная сумма файла для обнаружения повреждения артефакта."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_manifest(root: Path, version: str) -> dict:
    """Проверить имя версии, контракт признаков и контрольные суммы файлов."""
    if not re.fullmatch(r"[0-9a-f]{64}", version):
        raise ValueError("Некорректная версия модели")
    folder = Path(root) / version
    manifest = json.loads((folder / "manifest.json").read_text())
    if manifest["model_version"] != version or manifest["features"] != FEATURE_COLUMNS:
        raise ValueError("Версия или признаки модели не совпадают")
    if manifest["source"]["features.py"] != digest(Path(__file__).with_name("features.py")):
        raise ValueError("Изменился код признаков: обучите новую версию модели")
    current_versions = {name: importlib.metadata.version(name) for name in manifest["versions"]}
    if current_versions != manifest["versions"]:
        raise ValueError("Версии ML-библиотек не совпадают с обучением")
    for filename in ("model.cbm", "scoring_input.json.gz"):
        if digest(folder / filename) != manifest["files"][filename]:
            raise ValueError(f"Не совпадает контрольная сумма: {filename}")
    return manifest


def train_artifact(
    frame: pd.DataFrame, root: Path, *, max_iterations: int = 1500, early_stopping_rounds: int = 100
) -> dict:
    """Выбрать число деревьев внутри train и сохранить модель атомарно по версии."""
    root = Path(root)
    parts, report = split_dataset(frame)
    if len(parts["train"]) < 2 or len(parts["test"]) < 2:
        raise ValueError("Для обучения и оценки нужны минимум две строки train и test")
    versions = {
        name: importlib.metadata.version(name)
        for name in ("catboost", "numpy", "pandas", "scikit-learn")
    }
    source_files = [
        Path(__file__),
        Path(__file__).with_name("features.py"),
        Path(__file__).with_name("data.py"),
        Path(__file__).resolve().parents[2] / "sql/queries/ml_prices.sql",
    ]
    provenance = {str(path.name): digest(path) for path in source_files}
    # Стабильный порядок строк и столбцов нужен и для обучения, и для версии снимка.
    snapshot = pd.concat([part.assign(prediction_set=name) for name, part in parts.items()])
    snapshot = snapshot.sort_values("price_id").reset_index(drop=True)
    snapshot_json = snapshot.to_json(orient="table", date_format="iso", double_precision=15)
    fingerprint = hashlib.sha256(snapshot_json.encode()).hexdigest()
    identity = {
        "data_sha256": fingerprint,
        "versions": versions,
        "source": provenance,
        "params": MODEL_PARAMS,
        "max_iterations": max_iterations,
        "early_stopping_rounds": early_stopping_rounds,
        "features": FEATURE_COLUMNS,
        "as_of": AS_OF.date().isoformat(),
    }
    version = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    folder = root / version
    if folder.exists():
        return load_manifest(root, version)
    train = parts["train"].sort_values("price_id")
    test = parts["test"].sort_values("price_id")
    X_train, y_train = make_features(train), train[TARGET]
    X_test, y_test = make_features(test), test[TARGET]
    fit = train.loading_date < VALIDATION_START
    if fit.sum() < 2 or (~fit).sum() < 2:
        raise ValueError("Недостаточно данных для внутренней временной validation")
    selector = CatBoostRegressor(**MODEL_PARAMS, iterations=max_iterations)
    selector.fit(
        X_train.loc[fit],
        y_train.loc[fit],
        cat_features=CATEGORICAL_FEATURES,
        eval_set=(X_train.loc[~fit], y_train.loc[~fit]),
        early_stopping_rounds=early_stopping_rounds,
        use_best_model=True,
        verbose=False,
    )
    iterations = selector.get_best_iteration() + 1
    model = CatBoostRegressor(**MODEL_PARAMS, iterations=iterations)
    model.fit(X_train, y_train, cat_features=CATEGORICAL_FEATURES, verbose=False)
    scores = {
        "train": metrics(y_train, model.predict(X_train)),
        "test": metrics(y_test, model.predict(X_test)),
        "baseline_test": metrics(y_test, np.full(len(y_test), y_train.mean())),
    }
    manifest = {
        **identity,
        "model_version": version,
        "model_name": "CatBoostRegressor",
        "trained_at": datetime.now(UTC).isoformat(),
        "iterations": iterations,
        "train_start": TRAIN_START.date().isoformat(),
        "test_start": TEST_START.date().isoformat(),
        "split_counts": report,
        "metrics": scores,
        "categorical_features": CATEGORICAL_FEATURES,
    }
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".training-", dir=root) as temporary:
        temporary = Path(temporary)
        model.save_model(str(temporary / "model.cbm"))
        scoring = snapshot.loc[snapshot.prediction_set != "train"]
        scoring.to_json(
            temporary / "scoring_input.json.gz",
            orient="table",
            date_format="iso",
            double_precision=15,
            compression="gzip",
        )
        manifest["files"] = {
            name: digest(temporary / name) for name in ("model.cbm", "scoring_input.json.gz")
        }
        (temporary / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
        )
        # Публикуем только полностью записанный каталог; вызывающий DAG сериализован.
        temporary.rename(folder)
    logger.info("Обучена модель %s; метрики=%s", version, scores)
    return manifest


def predict_artifact(root: Path, version: str) -> pd.DataFrame:
    """Скоринг test и future из неизменяемого снимка; будущее без факта и ошибок."""
    manifest = load_manifest(root, version)
    folder = Path(root) / version
    with gzip.open(folder / "scoring_input.json.gz", "rt") as stream:
        frame = pd.read_json(stream, orient="table", precise_float=True)
    model = CatBoostRegressor()
    model.load_model(str(folder / "model.cbm"))
    if model.feature_names_ != FEATURE_COLUMNS:
        raise ValueError("Признаки сохранённой модели не совпадают")
    result = frame[
        [
            "price_id",
            "loading_date",
            "loading_city_gcid",
            "unloading_city_gcid",
            "body_group_ids",
            "prediction_set",
        ]
    ].copy()
    if frame.empty:
        predicted = np.empty(0)
    else:
        predicted = model.predict(make_features(frame))
    if not np.isfinite(predicted).all():
        raise ValueError("Неконечный прогноз модели")
    result["actual_price_rub"] = frame[TARGET].where(frame.prediction_set == "test")
    result["predicted_price_rub"] = predicted
    result["abs_error_rub"] = (result.predicted_price_rub - result.actual_price_rub).abs()
    result["ape_pct"] = 100 * result.abs_error_rub / result.actual_price_rub
    result["model_version"] = version
    result["model_name"] = manifest["model_name"]
    result["as_of"] = AS_OF.date()
    return result
