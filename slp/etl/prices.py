
"""Incremental ETL: public.prices -> core.prices + dq.quarantine_prices.

Processes records changed within [start_date, end_date).

The source timestamps are PostgreSQL TIMESTAMP WITHOUT TIME ZONE.
Airflow interval boundaries are normalized to naive UTC timestamps.

Each batch is committed atomically:
- resolve obsolete DQ reasons;
- insert or reopen current DQ reasons;
- remove currently invalid records from Core;
- upsert valid records into Core.
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
    WHERE updated_at >= %s
      AND updated_at < %s
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


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def normalize_interval(
    start_date: datetime,
    end_date: datetime,
) -> tuple[datetime, datetime]:
    """Convert Airflow boundaries to naive UTC timestamps."""

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
    """Check that a numeric value is finite."""

    if value is None:
        return False

    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def calculate_weight_category(weight: float) -> str:
    """Derive weight category after rounding to 0.1 tonne."""

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
    """Derive volume category."""

    return "FTL" if 0 < volume <= 120 else "OTH"


def source_timestamp(row: dict) -> datetime:
    """Get effective change timestamp for a source record."""

    timestamp = row["updated_at"]

    if timestamp is None:
        timestamp = row["created_at"]

    if timestamp is None:
        raise ValueError(
            f"Source price {row['id']} has no change timestamp"
        )

    return timestamp


# ---------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------

def load_references(conn):
    """Load reference data into memory for fast DQ checks."""

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
# Transform + Data Quality
# ---------------------------------------------------------------------

def transform_price(
    row: dict,
    cities: set,
    rates: dict,
) -> tuple[dict | None, list[str]]:
    """Validate and enrich one source price.

    Returns:
        (clean_record, []) for valid records.
        (None, reasons) for invalid records.
    """

    reasons = []

    currency = str(row["currency_code"])

    price = row["price"]
    distance = row["distance"]
    weight = row["total_weight"]
    volume = row["total_volume"]

    # Price validation.
    if not is_finite(price) or price <= 0:
        reasons.append("INVALID_PRICE")

    elif currency == "643" and price <= 1000:
        reasons.append("LOW_RUB_PRICE")

    # Distance validation.
    if not is_finite(distance) or distance <= 0:
        reasons.append("INVALID_DISTANCE")

    # Weight validation.
    if not is_finite(weight) or weight < 0:
        reasons.append("NEGATIVE_WEIGHT")

    # Volume validation.
    if not is_finite(volume) or volume < 0:
        reasons.append("NEGATIVE_VOLUME")

    # City references.
    if row["loading_city_gcid"] not in cities:
        reasons.append("MISSING_LOADING_CITY")

    if row["unloading_city_gcid"] not in cities:
        reasons.append("MISSING_UNLOADING_CITY")

    # Exchange rate.
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

    # Fill only missing categories.
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
# Batch load
# ---------------------------------------------------------------------

def process_batch(
    conn,
    batch: list[dict],
    cities: set,
    rates: dict,
) -> dict:
    """Process one batch in the caller's transaction."""

    valid = []
    invalid_ids = []

    current_issues = set()
    reasons_count = Counter()

    # 1. Transform records.
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

    with conn.cursor() as cur:

        # 2. Load existing active DQ reasons.
        cur.execute("""
            SELECT price_id, reason_code
            FROM dq.quarantine_prices
            WHERE price_id = ANY(%s)
              AND is_resolved = FALSE
        """, (batch_ids,))

        previous_issues = set(cur.fetchall())

        # 3. Resolve reasons no longer present.
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

        # 4. Insert or reopen current issues.
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

        # 5. Remove invalid records from Core.
        if invalid_ids:
            cur.execute("""
                DELETE FROM core.prices
                WHERE id = ANY(%s)
            """, (invalid_ids,))

        # 6. Upsert valid records.
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
                INSERT_PRICES_SQL,
                values,
            )

    return {
        "processed": len(batch),
        "valid": len(valid),
        "invalid": len(invalid_ids),
        "resolved": len(resolved_issues),
        "reasons": reasons_count,
    }


# ---------------------------------------------------------------------
# ETL entry point
# ---------------------------------------------------------------------

def run_prices_etl(
    start_date: datetime,
    end_date: datetime,
) -> dict:
    """Process source records changed in [start_date, end_date)."""

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

            # Reference dictionaries.
            cities, rates = load_references(core_conn)

            core_conn.commit()

            # Server-side cursor: avoid loading all rows into memory.
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
        "reasons": dict(reason_totals),
    }

    logger.info(
        "Prices ETL completed: %s",
        summary,
    )

    return summary
