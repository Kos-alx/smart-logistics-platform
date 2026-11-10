"""Проверки загрузки Mart на существующих данных с откатом изменений."""

import os

import psycopg
import pytest


@pytest.fixture
def conn():
    if os.getenv("SLP_RUN_DB_TESTS") != "1":
        pytest.skip("Проверки БД отключены")
    connection = psycopg.connect(
        host=os.environ["SLP_CORE_DB_HOST"],
        port=int(os.getenv("SLP_DB_PORT", "5432")),
        dbname=os.environ["SLP_DB_NAME"],
        user=os.environ["SLP_DB_USER"],
        password=os.environ["SLP_DB_PASSWORD"],
    )
    try:
        connection.execute("DELETE FROM meta_data.mart_checkpoint")
        yield connection
    finally:
        connection.rollback()
        connection.close()


def test_full_incremental_and_deletion(conn):
    from slp.etl.prices_prepared import QUERY_PATH, load_prices_prepared

    # Проверяем запуск после чистой инициализации: Mart ещё не наполнена.
    conn.execute("DELETE FROM mart.prices_prepared")
    # При full журнал должен включить и исчезнувший из Core месяц.
    prepared_query = QUERY_PATH.read_text().strip().rstrip(";")
    conn.execute(
        "INSERT INTO mart.prices_prepared SELECT "
        "(jsonb_populate_record(NULL::mart.prices_prepared, to_jsonb(p) || "
        "jsonb_build_object('month', '2099-01-01', 'first_price_date', '2099-01-01'))).* "
        "FROM (" + prepared_query + ") p LIMIT 1"
    )
    expected_months = {row[0] for row in conn.execute(
        "SELECT DISTINCT date_trunc('month', d.price_date)::date "
        "FROM core.price_dates d JOIN core.prices p ON p.id = d.price_id "
        "UNION SELECT DATE '2099-01-01'"
    )}
    journal_start = conn.execute(
        "SELECT COALESCE(max(change_id), 0) FROM meta_data.mart_changed_months"
    ).fetchone()[0]

    def logged_months():
        return conn.execute(
            "SELECT month::text FROM meta_data.mart_changed_months "
            "WHERE change_id > %s ORDER BY change_id", (journal_start,)
        ).fetchall()

    full = load_prices_prepared(conn)
    assert full["mode"] == "full"
    assert full["inserted"] > 0
    full_months = conn.execute(
        "SELECT month FROM meta_data.mart_changed_months WHERE change_id > %s",
        (journal_start,),
    ).fetchall()
    assert len(full_months) == len(expected_months)
    assert {row[0] for row in full_months} == expected_months
    full_log = logged_months()
    baseline = conn.execute(
        "SELECT count(*), sum(price_rub) FROM mart.prices_prepared"
    ).fetchone()
    repeat = load_prices_prepared(conn)
    assert repeat["mode"] == "incremental"
    assert repeat["events"] == repeat["inserted"] == repeat["deleted"] == 0
    assert logged_months() == full_log
    journal_start = conn.execute(
        "SELECT max(change_id) FROM meta_data.mart_changed_months"
    ).fetchone()[0]

    # Новая цена и её связи должны появиться в Mart без полной пересборки.
    conn.execute(
        "INSERT INTO core.prices SELECT "
        "(jsonb_populate_record(NULL::core.prices, "
        "to_jsonb(p) || jsonb_build_object('id', -99999999))).* "
        "FROM (SELECT * FROM core.prices ORDER BY id LIMIT 1) p"
    )
    conn.execute(
        "INSERT INTO core.price_dates(id, price_id, price_date) "
        "VALUES (-999998, -99999999, '2027-11-15')"
    )
    conn.execute(
        "INSERT INTO meta_data.changed_prices(price_id, source_table) "
        "VALUES (-99999999, 'prices')"
    )
    added = load_prices_prepared(conn)
    assert added["changed_prices"] == added["inserted"] == 1
    assert logged_months() == [("2027-11-01",)]
    journal_start = conn.execute(
        "SELECT max(change_id) FROM meta_data.mart_changed_months"
    ).fetchone()[0]
    assert conn.execute(
        "SELECT count(*) FROM mart.prices_prepared WHERE price_id = -99999999"
    ).fetchone()[0] == 1

    price_id = conn.execute("SELECT min(price_id) FROM mart.prices_prepared").fetchone()[0]
    conn.execute(
        "UPDATE core.prices SET price_rub = price_rub + 100 WHERE id = %s", (price_id,)
    )
    # Удаляем старые даты и кузова, чтобы проверить смену ключа Mart.
    conn.execute("DELETE FROM core.price_dates WHERE price_id = %s", (price_id,))
    conn.execute(
        "INSERT INTO core.price_dates(id, price_id, price_date) "
        "VALUES (-999999, %s, '2027-12-15')", (price_id,)
    )
    conn.execute("DELETE FROM core.price_body_types WHERE price_id = %s", (price_id,))
    conn.execute("UPDATE core.prices SET body_type = NULL WHERE id = %s", (price_id,))
    conn.execute(
        "INSERT INTO meta_data.changed_prices(price_id, source_table) "
        "VALUES (%s, 'prices'), (%s, 'price_dates'), (%s, 'price_body_types')",
        (price_id, price_id, price_id),
    )
    incremental = load_prices_prepared(conn)
    assert incremental["events"] == 3
    assert incremental["changed_prices"] == incremental["inserted"] == 1
    assert logged_months() == [("2027-11-01",), ("2027-12-01",)]
    rows = conn.execute(
        "SELECT month::text, first_price_date::text, body_group_id, price_rub "
        "FROM mart.prices_prepared WHERE price_id = %s", (price_id,)
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][:3] == ("2027-12-01", "2027-12-15", -2)
    assert rows[0][3] == conn.execute(
        "SELECT price_rub FROM core.prices WHERE id = %s", (price_id,)
    ).fetchone()[0]
    previous_log = logged_months()
    assert load_prices_prepared(conn)["events"] == 0
    assert logged_months() == previous_log
    journal_start = conn.execute(
        "SELECT max(change_id) FROM meta_data.mart_changed_months"
    ).fetchone()[0]

    conn.execute("DELETE FROM core.prices WHERE id = %s", (price_id,))
    conn.execute(
        "INSERT INTO meta_data.changed_prices(price_id, source_table) VALUES (%s, 'prices')",
        (price_id,),
    )
    removed = load_prices_prepared(conn)
    assert removed["deleted"] == 1 and removed["inserted"] == 0
    assert logged_months() == [("2027-12-01",)]
    assert conn.execute(
        "SELECT count(*) FROM mart.prices_prepared WHERE price_id = %s", (price_id,)
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT count(*), sum(price_rub) FROM mart.prices_prepared"
    ).fetchone() == baseline


def test_failure_rolls_back_mart_and_checkpoint(conn, monkeypatch, tmp_path):
    from slp.etl import prices_prepared

    prices_prepared.load_prices_prepared(conn)
    state = conn.execute("SELECT * FROM meta_data.mart_checkpoint").fetchall()
    months_before = conn.execute(
        "SELECT * FROM meta_data.mart_changed_months ORDER BY change_id"
    ).fetchall()
    fingerprint_sql = (
        "SELECT count(*), sum(hashtextextended(to_jsonb(p)::text, 0)::numeric) "
        "FROM mart.prices_prepared p"
    )
    before = conn.execute(fingerprint_sql).fetchone()
    broken = tmp_path / "broken.sql"
    query = prices_prepared.QUERY_PATH.read_text().strip().rstrip(";")
    # Ошибка PK возникает при INSERT после DELETE, а не при создании staging.
    broken.write_text(
        "SELECT p.* FROM (" + query + ") p CROSS JOIN generate_series(1, 2)",
        encoding="utf-8",
    )
    monkeypatch.setattr(prices_prepared, "QUERY_PATH", broken)
    with pytest.raises(psycopg.Error):
        prices_prepared.load_prices_prepared(conn, mode="full")
    assert conn.execute("SELECT * FROM meta_data.mart_checkpoint").fetchall() == state
    assert conn.execute(fingerprint_sql).fetchone() == before
    assert conn.execute(
        "SELECT * FROM meta_data.mart_changed_months ORDER BY change_id"
    ).fetchall() == months_before

    # Сбой после записи месяцев, при сохранении checkpoint: откатываются все слои.
    monkeypatch.undo()
    conn.execute("""
        CREATE FUNCTION pg_temp.reject_mart_checkpoint() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'Тестовая ошибка сохранения checkpoint';
        END;
        $$
    """)
    conn.execute("""
        CREATE TRIGGER reject_mart_checkpoint
        BEFORE UPDATE ON meta_data.mart_checkpoint
        FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_mart_checkpoint()
    """)
    with pytest.raises(psycopg.errors.RaiseException, match="Тестовая ошибка"):
        prices_prepared.load_prices_prepared(conn, mode="full")
    assert conn.execute("SELECT * FROM meta_data.mart_checkpoint").fetchall() == state
    assert conn.execute(fingerprint_sql).fetchone() == before
    assert conn.execute(
        "SELECT * FROM meta_data.mart_changed_months ORDER BY change_id"
    ).fetchall() == months_before
