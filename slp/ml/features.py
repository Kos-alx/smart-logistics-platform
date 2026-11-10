"""Признаки и временные выборки ML без статистик целевого тарифа."""

import numpy as np
import pandas as pd

AS_OF = pd.Timestamp("2026-10-26")
TRAIN_START = pd.Timestamp("2026-07-26")
TEST_START = pd.Timestamp("2026-10-05")
VALIDATION_START = pd.Timestamp("2026-09-21")

NUMERIC_FEATURES = [
    "distance",
    "total_weight",
    "total_volume",
    "origin_latitude",
    "origin_longitude",
    "destination_latitude",
    "destination_longitude",
    "loading_year",
    "loading_month",
    "loading_day_of_week",
    "loading_day_of_year",
    "month_sin",
    "month_cos",
    "weekday_sin",
    "weekday_cos",
    "day_of_year_sin",
    "day_of_year_cos",
    "body_tent",
    "body_reefer",
    "body_isotherm",
    "body_board",
    "body_other",
    "body_unspecified",
]
CATEGORICAL_FEATURES = ["origin_region", "destination_region", "weight_category", "volume_category"]
FEATURE_COLUMNS = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET = "price_rub"


def make_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Подготовить признаки одной сделки без доступа к таргету."""
    result = frame[
        [
            "distance",
            "total_weight",
            "total_volume",
            "origin_latitude",
            "origin_longitude",
            "destination_latitude",
            "destination_longitude",
            *CATEGORICAL_FEATURES,
        ]
    ].copy()
    dates = frame["loading_date"].dt
    result["loading_year"] = dates.year
    result["loading_month"] = dates.month
    result["loading_day_of_week"] = dates.dayofweek
    result["loading_day_of_year"] = dates.dayofyear
    for name, values, period in [
        ("month", dates.month - 1, 12),
        ("weekday", dates.dayofweek, 7),
        ("day_of_year", dates.dayofyear - 1, 365.25),
    ]:
        result[f"{name}_sin"] = np.sin(2 * np.pi * values / period)
        result[f"{name}_cos"] = np.cos(2 * np.pi * values / period)
    for group_id, name in {
        56: "tent",
        4: "reefer",
        2: "isotherm",
        15: "board",
        -1: "other",
        -2: "unspecified",
    }.items():
        result[f"body_{name}"] = frame["body_group_ids"].map(
            lambda groups, group_id=group_id: int(group_id in groups)
        )
    for column in CATEGORICAL_FEATURES:
        result[column] = result[column].fillna("Не указан").astype(str)
    return result[FEATURE_COLUMNS]


def split_dataset(frame: pd.DataFrame) -> tuple[dict, dict]:
    """Разделить историю и будущие заявки с разными правилами доступности таргета."""
    if frame.price_id.isna().any() or not frame.price_id.is_unique:
        raise ValueError("Нужен уникальный непустой price_id")
    frame = frame.copy()
    frame["loading_date"] = pd.to_datetime(frame["loading_date"])
    frame[TARGET] = pd.to_numeric(frame[TARGET], errors="raise")
    valid = (
        np.isfinite(frame.distance)
        & (frame.distance > 0)
        & np.isfinite(frame.total_weight)
        & (frame.total_weight >= 0)
        & np.isfinite(frame.total_volume)
        & (frame.total_volume >= 0)
        & ~frame.complex
    ).fillna(False)
    historical = (
        valid
        & frame.is_fact
        & frame.price_type.isin([1, 2, 3, 4, 5, 9])
        & ~frame.outlier_mark
        & np.isfinite(frame[TARGET])
        & (frame[TARGET] > 0)
    ).fillna(False)
    windows = {
        "train": (frame.loading_date >= TRAIN_START) & (frame.loading_date < TEST_START),
        "test": (frame.loading_date >= TEST_START) & (frame.loading_date <= AS_OF),
        "future": frame.loading_date > AS_OF,
    }
    parts, report = {}, {}
    for name, window in windows.items():
        parts[name] = frame.loc[window & (valid if name == "future" else historical)].copy()
        report[name] = {"before_filters": int(window.sum()), "rows": len(parts[name])}
    parts["future"][TARGET] = np.nan
    return parts, report
