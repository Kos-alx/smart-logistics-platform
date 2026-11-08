
import logging
import math

from psycopg.rows import dict_row

from slp.db import get_connection

logger = logging.getLogger(__name__)

SUPPORTED_CURRENCIES = {"643", "840", "978"}


def extract_exchange_rates() -> list[dict]:
    with get_connection("SLP_RAW_DB_HOST") as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT id, num_code, value, setting_date
                FROM public.exchange_rates
                ORDER BY setting_date, num_code, id
            """)
            rows = cur.fetchall()

    logger.info("Extracted %s exchange rates", len(rows))
    return rows


def transform_exchange_rates(rows: list[dict]) -> list[dict]:
    unique = {}

    for row in rows:
        code = row["num_code"]
        value = row["value"]
        rate_date = row["setting_date"]

        if code not in SUPPORTED_CURRENCIES:
            raise ValueError(f"Unsupported currency: {code}")

        if not math.isfinite(value) or value <= 0:
            raise ValueError(
                f"Invalid exchange rate: id={row['id']}, value={value}"
            )

        if code == "643" and value != 1.0:
            raise ValueError(
                f"Invalid RUB exchange rate: id={row['id']}"
            )

        key = (code, rate_date)

        # Если в Raw есть несколько курсов на одну дату,
        # выбираем запись с максимальным id.
        if key not in unique or row["id"] > unique[key]["id"]:
            unique[key] = row

    result = list(unique.values())

    logger.info(
        "Exchange rates transformed: input=%s, output=%s, duplicates=%s",
        len(rows),
        len(result),
        len(rows) - len(result),
    )

    return result


def load_exchange_rates(rows: list[dict]) -> int:
    if not rows:
        return 0

    query = """
        INSERT INTO core.exchange_rates
            (id, num_code, value, setting_date)
        VALUES
            (%(id)s, %(num_code)s, %(value)s, %(setting_date)s)
        ON CONFLICT (num_code, setting_date)
        DO UPDATE SET
            value = EXCLUDED.value
    """

    batch_size = 1000

    with get_connection("SLP_CORE_DB_HOST") as conn:
        with conn.cursor() as cur:
            for offset in range(0, len(rows), batch_size):
                batch = rows[offset:offset + batch_size]
                cur.executemany(query, batch)

    logger.info("Loaded %s exchange rates", len(rows))
    return len(rows)


def run_exchange_rates_etl() -> dict:
    extracted = extract_exchange_rates()
    transformed = transform_exchange_rates(extracted)
    loaded = load_exchange_rates(transformed)

    stats = {
        "extracted": len(extracted),
        "duplicates": len(extracted) - len(transformed),
        "loaded": loaded,
    }

    logger.info("Exchange rates ETL completed: %s", stats)
    return stats
