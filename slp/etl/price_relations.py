"""Синхронизация связей цен из внешнего источника в Core.

Источник и Core считаются независимыми базами данных.
Снимок источника передаётся пакетами через Python во временную таблицу Core.
После полной загрузки изменения применяются одной транзакцией вместе
с записью событий для последующего обновления Mart.
"""

import logging

from psycopg.rows import dict_row

from slp.db import get_connection

logger = logging.getLogger(__name__)

BATCH_SIZE = 5_000

# Допустимые таблицы и соответствующие колонки значений.
# Имена подставляются в SQL только из этого фиксированного словаря.
RELATIONS = {
    "price_dates": "price_date",
    "price_body_types": "body_type_id",
}


def load_relation(table_name: str) -> dict:
    """Синхронизировать полный снимок одной таблицы связей."""
    if table_name not in RELATIONS:
        raise ValueError(f"Неизвестная таблица связей: {table_name}")

    value_column = RELATIONS[table_name]
    value_type = "DATE" if table_name == "price_dates" else "INTEGER"

    # Имена таблиц и колонок берутся только из RELATIONS.
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

    # Подключения независимы: SQL-запросы к источнику не используют Core.
    with (
        get_connection("SLP_RAW_DB_HOST") as raw_conn,
        get_connection("SLP_CORE_DB_HOST") as core_conn,
    ):
        with raw_conn.cursor() as cur:
            # Повторяемое чтение обеспечивает согласованность полного снимка.
            cur.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )

        with core_conn.cursor() as core_cur:
            # Временная таблица существует только в текущей транзакции Core.
            # До завершения загрузки исходная таблица Core не изменяется.
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

            # Серверный курсор не загружает весь снимок в память Python.
            with raw_conn.cursor(
                name="relation_source_stream",
                row_factory=dict_row,
            ) as raw_cur:
                raw_cur.execute(extract_sql)

                while rows := raw_cur.fetchmany(BATCH_SIZE):
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

            # Сравниваем по первичному ключу и содержимому строки.
            # Для изменённого id сохраняем как старый, так и новый price_id:
            # связь могла быть перенесена с одной цены на другую.
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

            # Фиксируем затронутые цены до изменения таблицы Core.
            core_cur.execute(f"""
                INSERT INTO meta_data.changed_prices (price_id, source_table)
                SELECT DISTINCT price_id, %s
                FROM (
                    SELECT old_price_id AS price_id FROM slp_relation_diff
                    UNION
                    SELECT new_price_id AS price_id FROM slp_relation_diff
                ) AS changed
                WHERE price_id IS NOT NULL
                RETURNING price_id
            """, (table_name,))
            stats["changed_prices"] = len(core_cur.fetchall())

            # Удаляем старые версии изменённых и отсутствующие в снимке строки.
            # Неизменившиеся строки не трогаем.
            core_cur.execute(f"""
                DELETE FROM core.{table_name} AS c
                USING slp_relation_diff AS d
                WHERE c.id = d.old_id
            """)

            # Вставляем новые версии и новые строки из полного снимка.
            core_cur.execute(f"""
                INSERT INTO core.{table_name} (id, price_id, {value_column})
                SELECT s.id, s.price_id, s.{value_column}
                FROM slp_relation_stage AS s
                JOIN slp_relation_diff AS d ON d.new_id = s.id
            """)

        # Коммит включает изменения Core и события журнала одновременно.
        core_conn.commit()

    logger.info("Синхронизация %s завершена: %s", table_name, stats)
    return stats
