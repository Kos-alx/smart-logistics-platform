import logging

from psycopg.rows import dict_row

from slp.db import get_connection

logger = logging.getLogger(__name__)

BATCH_SIZE = 5_000

# Фиксированный набор допустимых таблиц и колонок.
RELATIONS = {
    "price_dates": "price_date",
    "price_body_types": "body_type_id",
}


def load_relation(table_name: str) -> dict:
    if table_name not in RELATIONS:
        raise ValueError(f"Unsupported relation: {table_name}")

    value_column = RELATIONS[table_name]

    extract_sql = f"""
        SELECT id, price_id, {value_column}
        FROM public.{table_name}
        WHERE id > %s
        ORDER BY id
        LIMIT %s
    """

    insert_sql = f"""
        INSERT INTO core.{table_name}
            (id, price_id, {value_column})
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
    """

    stats = {
        "extracted": 0,
        "inserted": 0,
        "batches": 0,
    }

    last_id = 0

    with (
        get_connection("SLP_RAW_DB_HOST") as raw_conn,
        get_connection("SLP_CORE_DB_HOST") as core_conn,
    ):
        with (
            raw_conn.cursor(row_factory=dict_row) as raw_cur,
            core_conn.cursor() as core_cur,
        ):
            while True:
                raw_cur.execute(
                    extract_sql,
                    (last_id, BATCH_SIZE),
                )
                rows = raw_cur.fetchall()

                if not rows:
                    break

                last_id = rows[-1]["id"]

                values = [
                    (
                        row["id"],
                        row["price_id"],
                        row[value_column],
                    )
                    for row in rows
                ]

                core_cur.executemany(insert_sql, values)
                core_conn.commit()

                stats["extracted"] += len(rows)
                stats["batches"] += 1

                logger.info(
                    "%s: batch=%s, extracted=%s, last_id=%s",
                    table_name,
                    stats["batches"],
                    len(rows),
                    last_id,
                )

    # Фактическое количество строк после загрузки.
    with get_connection("SLP_CORE_DB_HOST") as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM core.{table_name}")
            stats["inserted"] = cur.fetchone()[0]

    logger.info("%s ETL completed: %s", table_name, stats)
    return stats
