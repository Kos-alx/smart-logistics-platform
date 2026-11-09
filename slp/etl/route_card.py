"""Обновление сохранённых карточек маршрута PostgreSQL и ClickHouse."""

import logging
from pathlib import Path

from slp.db import get_clickhouse_client, get_connection

logger = logging.getLogger(__name__)
SQL_ROOT = Path(__file__).resolve().parents[2] / "sql"


def refresh_pg_route_card() -> dict:
    """Создать представление при первом запуске и атомарно обновить результат."""
    with get_connection("SLP_CORE_DB_HOST") as conn:
        conn.execute("SELECT pg_advisory_xact_lock(7341003)")
        if conn.execute("SELECT to_regclass('mart.mart_route_card')").fetchone()[0] is None:
            conn.execute((SQL_ROOT / "mart/02_route_card.sql").read_text(encoding="utf-8"))
        conn.execute("REFRESH MATERIALIZED VIEW mart.mart_route_card")
        rows = conn.execute("SELECT count(*) FROM mart.mart_route_card").fetchone()[0]
    logger.info("Карточка маршрута PostgreSQL обновлена: строк=%s", rows)
    return {"database": "postgres", "rows": rows, "as_of": "2026-10-26"}


def refresh_ch_route_card() -> dict:
    """Обновить refreshable MV и остановить периодические обновления после расчёта."""
    ch = get_clickhouse_client()
    try:
        # Тот же advisory-lock, что у переноса PG → CH: данные не меняются во время расчёта.
        with get_connection("SLP_CORE_DB_HOST") as conn:
            conn.execute("SELECT pg_advisory_xact_lock(7341002)")
            ddl = (SQL_ROOT / "clickhouse/02_route_card.sql").read_text(encoding="utf-8")
            for statement in ddl.split(";"):
                if statement.strip():
                    ch.command(statement)
            try:
                ch.command("SYSTEM START VIEW mart_route_card")
                ch.command("SYSTEM REFRESH VIEW mart_route_card")
                ch.command("SYSTEM WAIT VIEW mart_route_card")
            finally:
                ch.command("SYSTEM STOP VIEW mart_route_card")
            rows = ch.query("SELECT count() FROM mart_route_card").first_row[0]
        logger.info("Карточка маршрута ClickHouse обновлена: строк=%s", rows)
        return {"database": "clickhouse", "rows": rows, "as_of": "2026-10-26"}
    finally:
        ch.close()
