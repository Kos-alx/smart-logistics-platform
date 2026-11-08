
import logging
import os

import psycopg
from psycopg.rows import dict_row

from slp.db import get_connection

logger = logging.getLogger(__name__)


def extract_cities() -> list[dict]:
    with get_connection("SLP_RAW_DB_HOST") as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT
                    id,
                    full_name_local,
                    full_name_gc,
                    gc_id,
                    name,
                    longitude,
                    latitude,
                    created_at,
                    updated_at
                FROM public.cities
                ORDER BY gc_id, id
            """)
            rows = cur.fetchall()

    logger.info("Extracted %s cities", len(rows))
    return rows


def transform_cities(rows: list[dict]) -> list[dict]:
    unique = {}

    for row in rows:
        gc_id = row["gc_id"]

        if gc_id not in unique or row["id"] < unique[gc_id]["id"]:
            unique[gc_id] = row

    result = list(unique.values())

    logger.info(
        "Cities transformed: input=%s, output=%s, duplicates=%s",
        len(rows),
        len(result),
        len(rows) - len(result),
    )

    return result


def load_cities(rows: list[dict]) -> int:
    if not rows:
        return 0

    columns = (
        "id",
        "full_name_local",
        "full_name_gc",
        "gc_id",
        "name",
        "longitude",
        "latitude",
        "created_at",
        "updated_at",
    )

    query = """
        INSERT INTO core.cities (
            id,
            full_name_local,
            full_name_gc,
            gc_id,
            name,
            longitude,
            latitude,
            created_at,
            updated_at
        )
        VALUES (
            %(id)s,
            %(full_name_local)s,
            %(full_name_gc)s,
            %(gc_id)s,
            %(name)s,
            %(longitude)s,
            %(latitude)s,
            %(created_at)s,
            %(updated_at)s
        )
        ON CONFLICT (gc_id)
        DO UPDATE SET
            id = EXCLUDED.id,
            full_name_local = EXCLUDED.full_name_local,
            full_name_gc = EXCLUDED.full_name_gc,
            name = EXCLUDED.name,
            longitude = EXCLUDED.longitude,
            latitude = EXCLUDED.latitude,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """

    with get_connection("SLP_CORE_DB_HOST") as conn:
        with conn.cursor() as cur:
            cur.executemany(query, rows)

    logger.info("Loaded %s cities into core.cities", len(rows))
    return len(rows)


def run_cities_etl() -> dict:
    extracted = extract_cities()
    transformed = transform_cities(extracted)
    loaded = load_cities(transformed)

    stats = {
        "extracted": len(extracted),
        "duplicates": len(extracted) - len(transformed),
        "loaded": loaded,
    }

    logger.info("Cities ETL completed: %s", stats)
    return stats
