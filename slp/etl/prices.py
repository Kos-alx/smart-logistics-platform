"""Инкрементальный ETL: public.prices -> core.prices и dq.quarantine_prices.

Обрабатывает записи, изменённые в интервале [start_date, end_date).

В источнике используются временные метки TIMESTAMP WITHOUT TIME ZONE.
Границы Airflow приводятся к UTC без часового пояса.

Каждый пакет сохраняется атомарно:
- закрываются неактуальные причины карантина;
- добавляются или открываются актуальные причины карантина;
- удаляются невалидные записи Core;
- вставляются или обновляются валидные записи Core;
- регистрируются фактические изменения Core для Mart.
"""

import logging
import math
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from psycopg import sql
from psycopg.rows import dict_row

from slp.db import get_connection


logger = logging.getLogger(__name__)

BATCH_SIZE = 5_000

SUPPORTED_CURRENCIES = {"643", "840", "978"}

RAW_COLUMNS = (
    "id",
    "created_at",
    "updated_at",
    "subscriber_id",
    "organization_id",
    "loading_date",
    "loading_city_gcid",
    "unloading_city_gcid",
    "distance",
    "total_weight",
    "total_volume",
    "body_type",
    "price",
    "currency_code",
    "complex",
    "price_type",
    "item_id",
    "is_fact",
    "nearest_rate_date",
    "meta_item_id",
    "end_loading_date",
    "source",
    "external_item_uuid",
    "outlier_mark",
    "weight_category",
    "volume_category",
    "price_group_id",
)

CORE_COLUMNS = (
    *RAW_COLUMNS,
    "exchange_rate",
    "price_rub",
    "processed_at",
)


# ---------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------

EXTRACT_PRICES_SQL = sql.SQL("""
    SELECT {columns}
    FROM public.prices
    WHERE COALESCE(updated_at, created_at) >= %s
        AND COALESCE(updated_at, created_at) < %s
    ORDER BY id
""").format(
    columns=sql.SQL(", ").join(
        map(sql.Identifier, RAW_COLUMNS)
    )
)



INSERT_PRICES_SQL = sql.SQL("""
    INSERT INTO core.prices ({columns})
    VALUES ({values})
    ON CONFLICT (id) DO UPDATE SET
        {updates}
    WHERE
        COALESCE(
            EXCLUDED.updated_at,
            EXCLUDED.created_at
        ) >= COALESCE(
            core.prices.updated_at,
            core.prices.created_at
        )
        AND ROW({current_fields})
            IS DISTINCT FROM ROW({incoming_fields})
""").format(
    columns=sql.SQL(", ").join(
        map(sql.Identifier, CORE_COLUMNS)
    ),
    values=sql.SQL(", ").join(
        sql.SQL("NOW()")
        if column == "processed_at"
        else sql.SQL("%s")
        for column in CORE_COLUMNS
    ),
    updates=sql.SQL(", ").join(
        sql.SQL("{column} = EXCLUDED.{column}").format(
            column=sql.Identifier(column)
        )
        for column in CORE_COLUMNS
        if column != "id"
    ),
    current_fields=sql.SQL(", ").join(
        sql.SQL("core.prices.{column}").format(
            column=sql.Identifier(column)
        )
        for column in CORE_COLUMNS
        if column not in ("id", "processed_at")
    ),
    incoming_fields=sql.SQL(", ").join(
        sql.SQL("EXCLUDED.{column}").format(
            column=sql.Identifier(column)
        )
        for column in CORE_COLUMNS
        if column not in ("id", "processed_at")
    ),
)



UPSERT_ISSUE_SQL = """
    INSERT INTO dq.quarantine_prices (
        price_id,
        loading_date,
        reason_code,
        processed_at,
        is_resolved,
        resolved_at
    )
    VALUES (%s, %s, %s, NOW(), FALSE, NULL)
    ON CONFLICT (price_id, reason_code)
    DO UPDATE SET
        loading_date = EXCLUDED.loading_date,
        processed_at = NOW(),
        is_resolved = FALSE,
        resolved_at = NULL
    WHERE dq.quarantine_prices.is_resolved = TRUE
"""


# Регистрация фактически изменённых цен для обновления Mart.
REGISTER_PRICE_CHANGES_SQL = """
    INSERT INTO meta_data.changed_prices (price_id, source_table)
    SELECT DISTINCT price_id, 'prices'
    FROM unnest(%s::bigint[]) AS changed(price_id)
"""


# ---------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------

def normalize_interval(
    start_date: datetime,
    end_date: datetime,
) -> tuple[datetime, datetime]:
    """Привести границы Airflow к UTC без часового пояса."""

    if not isinstance(start_date, datetime):
        raise TypeError("start_date must be datetime")
    if not isinstance(end_date, datetime):
        raise TypeError("end_date must be datetime")

    def to_naive_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("Airflow interval must be timezone-aware")

        return value.astimezone(timezone.utc).replace(tzinfo=None)

    start = to_naive_utc(start_date)
    end = to_naive_utc(end_date)

    if start >= end:
        raise ValueError("start_date must be earlier than end_date")

    return start, end


def is_finite(value) -> bool:
    """Проверить, что число конечно."""

    if value is None:
        return False

    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def calculate_weight_category(weight: float) -> str:
    """Определить категорию веса после округления до 0,1 тонны."""

    rounded = Decimal(str(weight)).quantize(
        Decimal("0.1"),
        rounding=ROUND_HALF_UP,
    )

    if rounded >= Decimal("30"):
        return "G20T"

    if rounded >= Decimal("10"):
        return "FTL"

    if rounded >= Decimal("5"):
        return "10T"

    if rounded >= Decimal("2.5"):
        return "5T"

    if rounded >= Decimal("0.5"):
        return "L5T"

    return "OTH"


def calculate_volume_category(volume: float) -> str:
    """Определить категорию объёма."""

    return "FTL" if 0 < volume <= 120 else "OTH"


def source_timestamp(row: dict) -> datetime:
    """Получить дату изменения записи источника."""

    timestamp = row["updated_at"]

    if timestamp is None:
        timestamp = row["created_at"]

    if timestamp is None:
        raise ValueError(
            f"Source price {row['id']} has no change timestamp"
        )

    return timestamp


# ---------------------------------------------------------------------
# Справочники
# ---------------------------------------------------------------------

def load_references(conn):
    """Загрузить справочники в память для проверок качества."""

    with conn.cursor() as cur:
        cur.execute("""
            SELECT gc_id
            FROM core.cities
        """)

        cities = {
            row[0]
            for row in cur.fetchall()
        }

        cur.execute("""
            SELECT num_code, setting_date, value
            FROM core.exchange_rates
        """)

        rates = {
            (str(code), setting_date): Decimal(str(value))
            for code, setting_date, value in cur.fetchall()
            if is_finite(value) and value > 0
        }

    if not cities:
        raise RuntimeError(
            "core.cities is empty; load cities first"
        )

    if not rates:
        raise RuntimeError(
            "core.exchange_rates is empty; load rates first"
        )

    return cities, rates


# ---------------------------------------------------------------------
# Преобразование и проверка качества
# ---------------------------------------------------------------------

def transform_price(
    row: dict,
    cities: set,
    rates: dict,
) -> tuple[dict | None, list[str]]:
    """Проверить и обогатить запись цены.

    Возвращает:
        (clean_record, []) для валидной записи.
        (None, reasons) для невалидной записи.
    """

    reasons = []

    currency = str(row["currency_code"])

    price = row["price"]
    distance = row["distance"]
    weight = row["total_weight"]
    volume = row["total_volume"]

    # Проверка цены.
    if not is_finite(price) or price <= 0:
        reasons.append("INVALID_PRICE")

    elif currency == "643" and price <= 1000:
        reasons.append("LOW_RUB_PRICE")

    # Проверка расстояния.
    if not is_finite(distance) or distance <= 0:
        reasons.append("INVALID_DISTANCE")

    # Проверка веса.
    if not is_finite(weight) or weight < 0:
        reasons.append("NEGATIVE_WEIGHT")

    # Проверка объёма.
    if not is_finite(volume) or volume < 0:
        reasons.append("NEGATIVE_VOLUME")

    # Проверка городов по справочнику.
    if row["loading_city_gcid"] not in cities:
        reasons.append("MISSING_LOADING_CITY")

    if row["unloading_city_gcid"] not in cities:
        reasons.append("MISSING_UNLOADING_CITY")

    # Валютный курс.
    rate = None

    if currency not in SUPPORTED_CURRENCIES:
        reasons.append("UNSUPPORTED_CURRENCY")

    elif currency == "643":
        rate = Decimal("1")

    elif row["nearest_rate_date"] is None:
        reasons.append("MISSING_RATE_DATE")

    else:
        rate = rates.get(
            (currency, row["nearest_rate_date"])
        )

        if rate is None:
            reasons.append("MISSING_EXCHANGE_RATE")

    if reasons:
        return None, reasons

    clean = dict(row)

    # Заполняем только отсутствующие категории.
    if clean["weight_category"] is None:
        clean["weight_category"] = (
            calculate_weight_category(weight)
        )

    if clean["volume_category"] is None:
        clean["volume_category"] = (
            calculate_volume_category(volume)
        )

    clean["exchange_rate"] = rate

    clean["price_rub"] = (
        Decimal(str(price)) * rate
    )

    return clean, []


# ---------------------------------------------------------------------
# Пакетная загрузка
# ---------------------------------------------------------------------

def process_batch(
    conn,
    batch: list[dict],
    cities: set,
    rates: dict,
) -> dict:
    """Обработать пакет в транзакции вызывающего кода."""

    valid = []
    invalid_ids = []

    current_issues = set()
    reasons_count = Counter()

    # 1. Проверяем и преобразуем записи.
    for row in batch:
        clean, reasons = transform_price(
            row,
            cities,
            rates,
        )

        if reasons:
            invalid_ids.append(row["id"])

            for reason in reasons:
                current_issues.add(
                    (row["id"], reason)
                )
                reasons_count[reason] += 1

        else:
            valid.append(clean)

    batch_ids = [
        row["id"]
        for row in batch
    ]

    loading_dates = {
        row["id"]: row["loading_date"]
        for row in batch
    }

    changed_ids = set()

    with conn.cursor() as cur:

        # 2. Загружаем активные причины карантина.
        cur.execute("""
            SELECT price_id, reason_code
            FROM dq.quarantine_prices
            WHERE price_id = ANY(%s)
              AND is_resolved = FALSE
        """, (batch_ids,))

        previous_issues = set(cur.fetchall())

        # 3. Закрываем неактуальные причины.
        resolved_issues = (
            previous_issues - current_issues
        )

        if resolved_issues:
            cur.executemany("""
                UPDATE dq.quarantine_prices
                SET
                    is_resolved = TRUE,
                    resolved_at = NOW()
                WHERE price_id = %s
                  AND reason_code = %s
                  AND is_resolved = FALSE
            """, list(resolved_issues))

        # 4. Добавляем или открываем актуальные причины.
        if current_issues:
            cur.executemany(
                UPSERT_ISSUE_SQL,
                [
                    (
                        price_id,
                        loading_dates[price_id],
                        reason,
                    )
                    for price_id, reason in current_issues
                ],
            )

        # 5. Удаляем невалидные записи из Core.
        # В журнал попадают только действительно удалённые строки.
        if invalid_ids:
            cur.execute("""
                DELETE FROM core.prices
                WHERE id = ANY(%s)
                RETURNING id
            """, (invalid_ids,))
            changed_ids.update(row[0] for row in cur.fetchall())

        # 6. Вставляем или обновляем валидные записи.
        # Пакетный UPSERT с RETURNING сохраняет производительность.
        if valid:
            values = [
                tuple(
                    row[column]
                    for column in CORE_COLUMNS
                    if column != "processed_at"
                )
                for row in valid
            ]

            cur.executemany(
                INSERT_PRICES_SQL + sql.SQL(" RETURNING id"),
                values,
                returning=True,
            )

            # У каждой операции отдельный набор результатов.
            # При отсутствии изменений RETURNING возвращает пустой набор.
            for index in range(len(values)):
                changed_ids.update(row[0] for row in cur.fetchall())
                if index < len(values) - 1 and not cur.nextset():
                    raise RuntimeError(
                        "Отсутствует результат пакетного UPSERT"
                    )

        # 7. Записываем изменения в той же транзакции, что и Core.
        if changed_ids:
            cur.execute(
                REGISTER_PRICE_CHANGES_SQL,
                (sorted(changed_ids),),
            )

    return {
        "processed": len(batch),
        "valid": len(valid),
        "invalid": len(invalid_ids),
        "resolved": len(resolved_issues),
        "changed": len(changed_ids),
        "reasons": reasons_count,
    }


# ---------------------------------------------------------------------
# Точка входа ETL
# ---------------------------------------------------------------------

def run_prices_etl(
    start_date: datetime,
    end_date: datetime,
) -> dict:
    """Обработать изменения источника за [start_date, end_date)."""

    start, end = normalize_interval(
        start_date,
        end_date,
    )

    logger.info(
        "Starting prices ETL for [%s, %s)",
        start,
        end,
    )

    totals = Counter()
    reason_totals = Counter()

    with get_connection("SLP_RAW_DB_HOST") as raw_conn:
        with get_connection("SLP_CORE_DB_HOST") as core_conn:

            # Справочники для проверок качества.
            cities, rates = load_references(core_conn)

            core_conn.commit()

            # Серверный курсор: не загружаем все строки в память.
            with raw_conn.cursor(
                name="prices_stream",
                row_factory=dict_row,
            ) as raw_cur:

                raw_cur.execute(
                    EXTRACT_PRICES_SQL,
                    (start, end),
                )

                while batch := raw_cur.fetchmany(BATCH_SIZE):

                    try:
                        result = process_batch(
                            core_conn,
                            batch,
                            cities,
                            rates,
                        )

                        core_conn.commit()

                    except Exception:
                        core_conn.rollback()
                        logger.exception(
                            "Prices ETL batch failed "
                            "for interval [%s, %s)",
                            start,
                            end,
                        )
                        raise

                    totals.update({
                        key: result[key]
                        for key in (
                            "processed",
                            "valid",
                            "invalid",
                            "resolved",
                            "changed",
                        )
                    })

                    reason_totals.update(
                        result["reasons"]
                    )

                    logger.info(
                        "Prices ETL progress: "
                        "processed=%s valid=%s invalid=%s",
                        totals["processed"],
                        totals["valid"],
                        totals["invalid"],
                    )

    summary = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "processed": totals["processed"],
        "valid": totals["valid"],
        "invalid": totals["invalid"],
        "resolved": totals["resolved"],
        "changed": totals["changed"],
        "reasons": dict(reason_totals),
    }

    logger.info(
        "Prices ETL completed: %s",
        summary,
    )

    return summary
