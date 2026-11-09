"""Синхронизация связей цен из независимого источника в Core.

Полный снимок источника передаётся пакетами через Python во временную таблицу.
Изменённые наборы связей обновляются атомарно вместе с журналом для Mart.
"""

import logging

from psycopg.rows import dict_row

from slp.db import get_connection

logger = logging.getLogger(__name__)

BATCH_SIZE = 5_000

# Имена таблиц и колонок берутся только из фиксированного списка.
RELATIONS = {
    "price_dates": "price_date",
    "price_body_types": "body_type_id",
}


def load_relation(table_name: str) -> dict:
    """Синхронизировать полный снимок таблицы связей с Core."""
    if table_name not in RELATIONS:
        raise ValueError(f"Неизвестная таблица связей: {table_name}")

    value_column = RELATIONS[table_name]
    value_type = "DATE" if table_name == "price_dates" else "INTEGER"

    extract_sql = f"""
        SELECT id, price_id, {value_column}
        FROM public.{table_name}
        ORDER BY id
    """

    stats = {
        "extracted": 0,
        "inserted": 0,
        "updated": 0,
        "deleted": 0,
        "changed_prices": 0,
        "batches": 0,
    }

    with (
        get_connection("SLP_RAW_DB_HOST") as raw_conn,
        get_connection("SLP_CORE_DB_HOST") as core_conn,
    ):
        with raw_conn.cursor() as raw_cur:
            # Все пакеты читаются из одного согласованного снимка Raw.
            raw_cur.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )

        with core_conn.cursor() as core_cur:
            # До полной загрузки снимка постоянные таблицы не меняются.
            core_cur.execute(f"""
                CREATE TEMP TABLE slp_relation_stage (
                    id BIGINT PRIMARY KEY,
                    price_id BIGINT NOT NULL,
                    {value_column} {value_type} NOT NULL
                ) ON COMMIT DROP
            """)

            insert_stage_sql = f"""
                INSERT INTO slp_relation_stage (id, price_id, {value_column})
                VALUES (%s, %s, %s)
            """

            with raw_conn.cursor(
                name="relation_source_stream",
                row_factory=dict_row,
            ) as source_cur:
                source_cur.execute(extract_sql)

                while rows := source_cur.fetchmany(BATCH_SIZE):
                    values = [
                        (row["id"], row["price_id"], row[value_column])
                        for row in rows
                    ]
                    core_cur.executemany(insert_stage_sql, values)
                    stats["extracted"] += len(values)
                    stats["batches"] += 1
                    logger.info(
                        "%s: пакет=%s, получено=%s, всего=%s",
                        table_name,
                        stats["batches"],
                        len(values),
                        stats["extracted"],
                    )

            # Сравнение по id учитывает вставки, удаления и изменения.
            # При переносе связи сохраняем оба price_id: старый и новый.
            core_cur.execute(f"""
                CREATE TEMP TABLE slp_relation_diff
                ON COMMIT DROP AS
                SELECT
                    c.id AS old_id,
                    c.price_id AS old_price_id,
                    s.id AS new_id,
                    s.price_id AS new_price_id
                FROM core.{table_name} AS c
                FULL OUTER JOIN slp_relation_stage AS s USING (id)
                WHERE c.id IS NULL
                   OR s.id IS NULL
                   OR c.price_id IS DISTINCT FROM s.price_id
                   OR c.{value_column} IS DISTINCT FROM s.{value_column}
            """)

            core_cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE old_id IS NULL),
                    COUNT(*) FILTER (
                        WHERE old_id IS NOT NULL AND new_id IS NOT NULL
                    ),
                    COUNT(*) FILTER (WHERE new_id IS NULL)
                FROM slp_relation_diff
            """)
            stats["inserted"], stats["updated"], stats["deleted"] = (
                core_cur.fetchone()
            )

            # Из различий собираем уникальные цены, требующие пересборки.
            core_cur.execute("""
                CREATE TEMP TABLE slp_changed_relation_prices
                ON COMMIT DROP AS
                SELECT DISTINCT price_id
                FROM (
                    SELECT old_price_id AS price_id FROM slp_relation_diff
                    UNION
                    SELECT new_price_id AS price_id FROM slp_relation_diff
                ) AS changed
                WHERE price_id IS NOT NULL
            """)

            core_cur.execute("""
                SELECT COUNT(*) FROM slp_changed_relation_prices
            """)
            stats["changed_prices"] = core_cur.fetchone()[0]

            # Регистрируем затронутые цены в той же транзакции.
            core_cur.execute("""
                INSERT INTO meta_data.changed_prices (price_id, source_table)
                SELECT price_id, %s
                FROM slp_changed_relation_prices
            """, (table_name,))

            # Удаляем все старые связи затронутых цен, включая неизменённые.
            # Это исключает конфликты уникальности при перестановке значений.
            core_cur.execute(f"""
                DELETE FROM core.{table_name} AS c
                USING slp_changed_relation_prices AS changed
                WHERE c.price_id = changed.price_id
            """)

            # Восстанавливаем полный актуальный набор связей этих цен.
            core_cur.execute(f"""
                INSERT INTO core.{table_name} (id, price_id, {value_column})
                SELECT s.id, s.price_id, s.{value_column}
                FROM slp_relation_stage AS s
                JOIN slp_changed_relation_prices AS changed
                  ON changed.price_id = s.price_id
            """)

        # Состояние Core и журнал изменений фиксируются одновременно.
        core_conn.commit()

    logger.info("Синхронизация %s завершена: %s", table_name, stats)
    return stats
