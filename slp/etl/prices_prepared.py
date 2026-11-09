"""Полная и инкрементальная загрузка подготовленного Mart из PostgreSQL Core."""

import logging
from pathlib import Path

from psycopg import sql

from slp.db import get_connection

logger = logging.getLogger(__name__)
QUERY_PATH = Path(__file__).resolve().parents[2] / "sql/queries/prices_prepared.sql"
PIPELINE_NAME = "prices_prepared"


def load_prices_prepared(conn, mode: str = "auto") -> dict:
    """Атомарно обновить Mart и checkpoint; auto выбирает full при первом запуске."""
    if mode not in ("auto", "full"):
        raise ValueError("Неизвестный режим: допустимы auto и full")
    query = QUERY_PATH.read_text(encoding="utf-8").strip().rstrip(";")

    with conn.transaction(), conn.cursor() as cur:
        # Сериализуем потребителей, в том числе запуски вне Airflow.
        cur.execute("SELECT pg_advisory_xact_lock(7341001)")
        # SHARE ждёт завершения текущих INSERT и блокирует новые до COMMIT.
        # Поэтому нижний change_id не сможет зафиксироваться после checkpoint.
        # Core и его события должны записываться в одной транзакции.
        cur.execute("LOCK TABLE meta_data.changed_prices IN SHARE MODE")
        cur.execute(
            "SELECT last_change_id FROM meta_data.mart_checkpoint "
            "WHERE pipeline_name = %s", (PIPELINE_NAME,)
        )
        state = cur.fetchone()
        full = mode == "full" or state is None
        previous = state[0] if state else 0
        cur.execute("SELECT COALESCE(MAX(change_id), 0) FROM meta_data.changed_prices")
        upper = max(previous, cur.fetchone()[0])
        cur.execute(
            "SELECT count(*), count(DISTINCT price_id) FROM meta_data.changed_prices "
            "WHERE change_id > %s AND change_id <= %s", (previous, upper)
        )
        events, changed_prices = cur.fetchone()
        stats = {
            "mode": "full" if full else "incremental",
            "events": events,
            "changed_prices": changed_prices,
            "deleted": 0,
            "inserted": 0,
            "months": 0,
            "checkpoint": upper,
        }
        if not full and events == 0:
            logger.info("Mart без изменений: %s", stats)
            return stats

        if not full:
            cur.execute(
                "CREATE TEMP TABLE slp_mart_changed ON COMMIT DROP AS "
                "SELECT DISTINCT price_id FROM meta_data.changed_prices "
                "WHERE change_id > %s AND change_id <= %s", (previous, upper)
            )
            cur.execute("CREATE UNIQUE INDEX ON slp_mart_changed(price_id)")
            cur.execute(
                "CREATE TEMP TABLE slp_mart_prices ON COMMIT DROP AS "
                "SELECT p.* FROM core.prices p "
                "JOIN slp_mart_changed c ON c.price_id = p.id"
            )
            cur.execute("ANALYZE slp_mart_prices")
            # Подменяем только фиксированное имя набора цен. SQL расчёта общий
            # для full и incremental, значения пользователя не интерполируются.
            query = query.replace("core.prices AS p", "pg_temp.slp_mart_prices AS p")

        cur.execute("CREATE TEMP TABLE slp_mart_stage ON COMMIT DROP AS " + query)
        cur.execute("SELECT * FROM slp_mart_stage LIMIT 0")
        columns = sql.SQL(", ").join(sql.Identifier(col.name) for col in cur.description)
        # Старые месяцы читаем до удаления: цена могла сменить месяц или исчезнуть.
        # UNION оставляет одно событие на месяц в пределах текущей загрузки.
        old_months = "SELECT month FROM mart.prices_prepared"
        if not full:
            old_months = (
                "SELECT p.month FROM mart.prices_prepared p "
                "JOIN slp_mart_changed c ON c.price_id = p.price_id"
            )
        cur.execute(
            "CREATE TEMP TABLE slp_mart_months ON COMMIT DROP AS "
            + old_months + " UNION SELECT month FROM slp_mart_stage"
        )
        if full:
            cur.execute("DELETE FROM mart.prices_prepared")
        else:
            cur.execute(
                "DELETE FROM mart.prices_prepared p USING slp_mart_changed c "
                "WHERE p.price_id = c.price_id"
            )
        stats["deleted"] = cur.rowcount
        cur.execute(
            sql.SQL("INSERT INTO mart.prices_prepared ({cols}) "
                    "SELECT {cols} FROM slp_mart_stage").format(cols=columns)
        )
        stats["inserted"] = cur.rowcount
        cur.execute(
            "INSERT INTO meta_data.mart_changed_months(month) "
            "SELECT month FROM slp_mart_months ORDER BY month"
        )
        stats["months"] = cur.rowcount
        cur.execute(
            "INSERT INTO meta_data.mart_checkpoint(pipeline_name, last_change_id) "
            "VALUES (%s, %s) ON CONFLICT(pipeline_name) DO UPDATE SET "
            "last_change_id = EXCLUDED.last_change_id, updated_at = CURRENT_TIMESTAMP",
            (PIPELINE_NAME, upper),
        )
        # Удаляем staging сразу: при тестовом внешнем transaction COMMIT ещё нет.
        cur.execute("DROP TABLE slp_mart_stage, slp_mart_months")
        if not full:
            cur.execute("DROP TABLE slp_mart_prices, slp_mart_changed")
    logger.info("Загрузка Mart завершена: %s", stats)
    return stats


def run_prices_prepared_etl(mode: str = "auto") -> dict:
    """Открыть соединение с Core и выполнить загрузку с фиксацией либо откатом."""
    if mode not in ("auto", "full"):
        raise ValueError("Неизвестный режим: допустимы auto и full")
    with get_connection("SLP_CORE_DB_HOST") as conn:
        return load_prices_prepared(conn, mode)
