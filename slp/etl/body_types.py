
import logging

from psycopg.rows import dict_row

from slp.db import get_connection

logger = logging.getLogger(__name__)


def extract_body_types() -> list[dict]:
    with get_connection("SLP_RAW_DB_HOST") as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT id, name, "id_from_1C", std_id
                FROM public.body_types
                ORDER BY id
            """)
            rows = cur.fetchall()

    logger.info("Extracted %s body types", len(rows))
    return rows


def transform_body_types(rows: list[dict]) -> list[dict]:
    ids = {row["id"] for row in rows}

    for row in rows:
        if row["std_id"] is not None and row["std_id"] not in ids:
            raise ValueError(
                f"Body type {row['id']} references "
                f"missing std_id={row['std_id']}"
            )

    return rows


def load_body_types(rows: list[dict]) -> int:
    if not rows:
        return 0

    # Сначала загружаем все id, не устанавливая std_id.
    # Это позволяет обработать ссылки на строки,
    # которые ещё не существовали в Core.
    insert_query = """
        INSERT INTO core.body_types (id, name, id_from_1c, std_id)
        VALUES (%(id)s, %(name)s, %(id_from_1C)s, NULL)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            id_from_1c = EXCLUDED.id_from_1c
    """

    update_query = """
        UPDATE core.body_types
        SET std_id = %(std_id)s
        WHERE id = %(id)s
    """

    with get_connection("SLP_CORE_DB_HOST") as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_query, rows)
            cur.executemany(update_query, rows)

    logger.info("Loaded %s body types", len(rows))
    return len(rows)


def run_body_types_etl() -> dict:
    extracted = extract_body_types()
    transformed = transform_body_types(extracted)
    loaded = load_body_types(transformed)

    stats = {
        "extracted": len(extracted),
        "loaded": loaded,
    }

    logger.info("Body types ETL completed: %s", stats)
    return stats
