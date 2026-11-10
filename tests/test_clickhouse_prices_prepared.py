"""Проверки партиционной синхронизации на отдельной тестовой таблице CH."""

import os
from datetime import date
from uuid import uuid4

import pytest


def test_partition_sync_and_retry(monkeypatch):
    if os.getenv("SLP_RUN_CH_TESTS") != "1":
        pytest.skip("Проверки ClickHouse отключены")
    from slp.db import get_clickhouse_client, get_connection
    from slp.etl import clickhouse_prices_prepared as etl

    table = "test_mart_" + uuid4().hex
    ch = get_clickhouse_client()
    conn = get_connection("SLP_CORE_DB_HOST")
    try:
        # Внешняя транзакция удерживает все изменения PG до итогового ROLLBACK.
        conn.execute("SELECT 1")
        full = etl.load_clickhouse_prices_prepared(conn, ch, table=table)
        assert full["mode"] == "full" and full["rows"] > 0
        baseline = ch.query(f"SELECT count(), sum(price_rub) FROM {table}").first_row
        repeat = etl.load_clickhouse_prices_prepared(conn, ch, table=table)
        assert repeat["months"] == repeat["rows"] == 0

        conn.execute("""
            INSERT INTO mart.prices_prepared
            SELECT (jsonb_populate_record(NULL::mart.prices_prepared, to_jsonb(p) ||
                jsonb_build_object('price_id', -99999999, 'month', '2099-10-01',
                                   'first_price_date', '2099-10-15'))).*
            FROM (SELECT * FROM mart.prices_prepared LIMIT 1) p
        """)
        conn.execute(
            "INSERT INTO meta_data.mart_changed_months(month) VALUES (%s)",
            (date(2099, 10, 1),),
        )
        added = etl.load_clickhouse_prices_prepared(conn, ch, table=table)
        assert added["months"] == added["rows"] == 1
        original = ch.query(
            f"SELECT price_rub FROM {table} WHERE price_id = -99999999"
        ).first_row[0]
        conn.execute(
            "UPDATE mart.prices_prepared SET price_rub=price_rub+100 "
            "WHERE price_id=-99999999"
        )
        conn.execute(
            "INSERT INTO meta_data.mart_changed_months(month) VALUES ('2099-10-01')"
        )
        etl.load_clickhouse_prices_prepared(conn, ch, table=table)
        assert ch.query(
            f"SELECT count(), sum(price_rub) FROM {table} WHERE price_id=-99999999"
        ).first_row == (1, original + 100)

        conn.execute("""
            UPDATE mart.prices_prepared
            SET month='2099-11-01', first_price_date='2099-11-15'
            WHERE price_id=-99999999
        """)
        conn.execute("""
            INSERT INTO meta_data.mart_changed_months(month)
            VALUES ('2099-10-01'), ('2099-11-01')
        """)
        moved = etl.load_clickhouse_prices_prepared(conn, ch, table=table)
        assert moved["months"] == 2 and moved["dropped"] == 1
        assert ch.query(
            f"SELECT count() FROM {table} WHERE month=toDate('2099-10-01')"
        ).first_row[0] == 0

        # Ошибка после первого из двух месяцев: checkpoint должен остаться прежним.
        before = conn.execute(
            "SELECT last_change_id FROM meta_data.mart_checkpoint WHERE pipeline_name=%s",
            (etl.checkpoint_name(ch, table),),
        ).fetchone()[0]
        conn.execute("""
            UPDATE mart.prices_prepared
            SET month='2099-12-01', first_price_date='2099-12-15'
            WHERE price_id=-99999999
        """)
        conn.execute("""
            INSERT INTO meta_data.mart_changed_months(month)
            VALUES ('2099-11-01'), ('2099-12-01')
        """)
        publish = etl.publish_month

        def fail_after_publish(*args, **kwargs):
            publish(*args, **kwargs)
            raise RuntimeError("Тестовый сбой после замены партиции")

        monkeypatch.setattr(etl, "publish_month", fail_after_publish)
        with pytest.raises(RuntimeError, match="Тестовый сбой"):
            etl.load_clickhouse_prices_prepared(conn, ch, table=table)
        assert conn.execute(
            "SELECT last_change_id FROM meta_data.mart_checkpoint WHERE pipeline_name=%s",
            (etl.checkpoint_name(ch, table),),
        ).fetchone()[0] == before
        monkeypatch.setattr(etl, "publish_month", publish)
        retried = etl.load_clickhouse_prices_prepared(conn, ch, table=table)
        assert retried["months"] == 2 and retried["rows"] == 1

        conn.execute("DELETE FROM mart.prices_prepared WHERE price_id=-99999999")
        conn.execute(
            "INSERT INTO meta_data.mart_changed_months(month) VALUES ('2099-12-01')"
        )
        removed = etl.load_clickhouse_prices_prepared(conn, ch, table=table)
        assert removed["dropped"] == 1 and removed["rows"] == 0
        assert ch.query(f"SELECT count(), sum(price_rub) FROM {table}").first_row == baseline

        # Full удаляет лишние партиции, даже если в PG для них нет событий.
        row = list(ch.query(f"SELECT {', '.join(etl.COLUMNS)} FROM {table} LIMIT 1").first_row)
        row[etl.COLUMNS.index("month")] = date(2099, 12, 1)
        row[etl.COLUMNS.index("first_price_date")] = date(2099, 12, 15)
        ch.insert(table, [row], column_names=etl.COLUMNS)
        rebuilt = etl.load_clickhouse_prices_prepared(conn, ch, mode="full", table=table)
        assert rebuilt["dropped"] == 1
        assert ch.query(f"SELECT count(), sum(price_rub) FROM {table}").first_row == baseline
    finally:
        conn.rollback()
        conn.close()
        ch.command(f"DROP TABLE IF EXISTS {table}_stage")
        ch.command(f"DROP TABLE IF EXISTS {table}")
        ch.close()
