"""Чтение данных Core для обучения, оценки и скоринга ML."""

from pathlib import Path

import pandas as pd

from slp.db import get_connection

SQL_ROOT = Path(__file__).resolve().parents[2] / "sql"


def load_ml_dataset() -> pd.DataFrame:
    """Прочитать батчами; итоговый DataFrame хранится в памяти."""
    batches = []
    with get_connection("SLP_CORE_DB_HOST") as conn:
        conn.read_only = True
        with conn.cursor(name="ml_core_snapshot") as cursor:
            cursor.execute((SQL_ROOT / "queries/ml_prices.sql").read_text())
            columns = [column.name for column in cursor.description]
            while rows := cursor.fetchmany(10_000):
                batches.append(pd.DataFrame.from_records(rows, columns=columns))
    frame = pd.concat(batches, ignore_index=True) if batches else pd.DataFrame(columns=columns)
    frame["loading_date"] = pd.to_datetime(frame["loading_date"])
    frame["price_rub"] = pd.to_numeric(frame["price_rub"])
    return frame
