"""Передача PG Mart в ClickHouse с проверкой и заменой месячных партиций."""

import logging
import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from slp.db import get_clickhouse_client, get_connection

logger = logging.getLogger(__name__)
DDL_PATH = Path(__file__).resolve().parents[2] / "sql/clickhouse/01_prices_prepared.sql"
BATCH_SIZE = 5_000
COLUMNS = (
    "price_id", "month", "first_price_date", "loading_date", "loading_city_gcid",
    "unloading_city_gcid", "origin_city", "destination_city", "distance", "total_weight",
    "total_volume", "weight_category", "volume_category", "body_group_id", "body_group_name",
    "price", "currency_code", "exchange_rate", "price_rub", "price_km_rub", "organization_id",
    "subscriber_id", "price_type", "is_fact", "complex", "outlier_mark", "created_at",
    "updated_at", "processed_at",
)


def checkpoint_name(ch, table: str) -> str:
    """Различать потребителей по базе и целевой таблице CH."""
    return f"clickhouse:{ch.database}:{table}"


def ensure_tables(ch, table: str) -> bool:
    """Создать таблицы по init-DDL и сообщить, существовала ли основная таблица."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError("Недопустимое имя таблицы")
    existed = int(ch.command(f"EXISTS TABLE {table}")) != 0
    ddl = re.sub(
        r"\bprices_prepared(_stage)?\b",
        lambda match: table + ("_stage" if match.group(1) else ""),
        DDL_PATH.read_text(encoding="utf-8"),
    )
    for statement in ddl.split(";"):
        if statement.strip():
            ch.command(statement)
    return existed


def month_metrics(ch, table: str, month) -> tuple:
    """Контроль количества, уникальности ключа и точной денежной суммы."""
    return ch.query(
        f"SELECT count(), uniqExact(tuple(price_id, month, body_group_id)), sum(price_rub) "
        f"FROM {table} WHERE month = {{month:Date}}", parameters={"month": month}
    ).first_row


def publish_month(ch, table: str, stage: str, month, rows: int):
    """Заменить готовую партицию либо удалить месяц, опустевший в PostgreSQL."""
    partition = month.year * 100 + month.month
    if rows:
        ch.command(f"ALTER TABLE {table} REPLACE PARTITION {partition} FROM {stage}")
    else:
        ch.command(f"ALTER TABLE {table} DROP PARTITION {partition}")


def load_clickhouse_prices_prepared(conn, ch, mode: str = "auto",
                                   table: str = "prices_prepared") -> dict:
    """Синхронизировать месяцы и сохранить checkpoint только после успешной проверки."""
    if mode not in ("auto", "full"):
        raise ValueError("Неизвестный режим: допустимы auto и full")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError("Недопустимое имя таблицы")
    stage = table + "_stage"
    pipeline = checkpoint_name(ch, table)
    column_sql = ", ".join(COLUMNS)
    with conn.transaction(), conn.cursor() as cur:
        # Один потребитель CH работает со staging. Ожидаем завершения писателей
        # журнала месяцев и не даём PG Mart измениться между чтениями партиций.
        cur.execute("SELECT pg_advisory_xact_lock(7341002)")
        cur.execute("LOCK TABLE meta_data.mart_changed_months IN SHARE MODE")
        existed = ensure_tables(ch, table)
        cur.execute(
            "SELECT last_change_id FROM meta_data.mart_checkpoint WHERE pipeline_name=%s",
            (pipeline,),
        )
        state = cur.fetchone()
        full = mode == "full" or state is None or not existed
        previous = state[0] if state else 0
        cur.execute("SELECT COALESCE(max(change_id), 0) FROM meta_data.mart_changed_months")
        upper = max(previous, cur.fetchone()[0])
        if full:
            cur.execute("SELECT DISTINCT month FROM mart.prices_prepared")
            months = {row[0] for row in cur.fetchall()}
            # Удаляем также лишние партиции CH, которых больше нет в PG.
            months.update(row[0] for row in ch.query(f"SELECT DISTINCT month FROM {table}").result_rows)
        else:
            cur.execute(
                "SELECT DISTINCT month FROM meta_data.mart_changed_months "
                "WHERE change_id>%s AND change_id<=%s", (previous, upper)
            )
            months = {row[0] for row in cur.fetchall()}
        stats = {"mode": "full" if full else "incremental", "months": len(months),
                 "rows": 0, "dropped": 0, "checkpoint": upper}
        if not full and not months:
            logger.info("ClickHouse без изменений: %s", stats)
            return stats

        for month in sorted(months):
            # Остатки неудачного запуска не смешиваются с новой загрузкой.
            ch.command(f"TRUNCATE TABLE {stage}")
            rows = 0
            amount = Decimal(0)
            with conn.cursor(name="slp_ch_export") as source:
                source.execute(
                    f"SELECT {column_sql} FROM mart.prices_prepared WHERE month=%s", (month,)
                )
                while batch := source.fetchmany(BATCH_SIZE):
                    # В PG created_at/updated_at не имеют timezone: трактуем их как UTC.
                    batch = [tuple(value.replace(tzinfo=UTC) if isinstance(value, datetime)
                                   and value.tzinfo is None else value for value in row)
                             for row in batch]
                    ch.insert(stage, batch, column_names=COLUMNS)
                    rows += len(batch)
                    amount += sum(row[COLUMNS.index("price_rub")] for row in batch)
            expected = (rows, rows, amount)
            if month_metrics(ch, stage, month) != expected:
                raise RuntimeError(f"Не совпали контрольные показатели staging: {month}")
            publish_month(ch, table, stage, month, rows)
            if month_metrics(ch, table, month) != expected:
                raise RuntimeError(f"Не совпали контрольные показатели партиции: {month}")
            stats["rows"] += rows
            stats["dropped"] += int(rows == 0)
            logger.info("Синхронизирован месяц %s: строк=%s", month, rows)
        ch.command(f"TRUNCATE TABLE {stage}")
        cur.execute(
            "INSERT INTO meta_data.mart_checkpoint(pipeline_name,last_change_id) "
            "VALUES (%s,%s) ON CONFLICT(pipeline_name) DO UPDATE SET "
            "last_change_id=EXCLUDED.last_change_id, updated_at=CURRENT_TIMESTAMP",
            (pipeline, upper),
        )
    logger.info("Синхронизация ClickHouse завершена: %s", stats)
    return stats


def run_clickhouse_prices_prepared_etl(mode: str = "auto") -> dict:
    """Открыть подключения и выполнить синхронизацию PG → CH."""
    if mode not in ("auto", "full"):
        raise ValueError("Неизвестный режим: допустимы auto и full")
    ch = get_clickhouse_client()
    try:
        with get_connection("SLP_CORE_DB_HOST") as conn:
            return load_clickhouse_prices_prepared(conn, ch, mode)
    finally:
        ch.close()
