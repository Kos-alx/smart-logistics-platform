"""Integration tests for conditional UPSERT in core.prices."""

import os
from datetime import timedelta
from decimal import Decimal

import psycopg
import pytest
from psycopg.rows import dict_row

from slp.etl.prices import CORE_COLUMNS, INSERT_PRICES_SQL


@pytest.fixture
def conn():
    if os.getenv("SLP_RUN_DB_TESTS") != "1":
        pytest.skip("Database tests are disabled")

    connection = psycopg.connect(
        host=os.environ["SLP_CORE_DB_HOST"],
        port=int(os.getenv("SLP_DB_PORT", "5432")),
        dbname=os.environ["SLP_DB_NAME"],
        user=os.environ["SLP_DB_USER"],
        password=os.environ["SLP_DB_PASSWORD"],
        row_factory=dict_row,
    )

    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()


def upsert(conn, record):
    values = tuple(
        record[column]
        for column in CORE_COLUMNS
        if column != "processed_at"
    )

    conn.execute(INSERT_PRICES_SQL, values)


def get_price(conn, price_id):
    return conn.execute(
        "SELECT * FROM core.prices WHERE id = %s",
        (price_id,),
    ).fetchone()


def test_conditional_upsert(conn):
    # Use an existing valid record as a fixture.
    original = conn.execute("""
        SELECT *
        FROM core.prices
        WHERE currency_code = '643'
          AND price > 10000
          AND updated_at IS NOT NULL
        ORDER BY id
        LIMIT 1
    """).fetchone()

    assert original is not None, "No suitable Core record"

    price_id = original["id"]
    initial_processed_at = original["processed_at"]

    # 1. Identical record: UPDATE must be skipped.
    upsert(conn, original)

    unchanged = get_price(conn, price_id)

    assert unchanged["price"] == original["price"]
    assert unchanged["processed_at"] == initial_processed_at

    # 2. Newer changed record: UPDATE must happen.
    changed = dict(original)
    changed["price"] = original["price"] + 1000.0
    changed["price_rub"] = original["price_rub"] + Decimal("1000")
    changed["updated_at"] = (
        original["updated_at"] + timedelta(seconds=1)
    )

    upsert(conn, changed)

    updated = get_price(conn, price_id)

    assert updated["price"] == changed["price"]
    assert updated["price_rub"] == changed["price_rub"]
    assert updated["updated_at"] == changed["updated_at"]
    assert updated["processed_at"] > initial_processed_at

    # 3. Repeat changed record: UPDATE must be skipped.
    upsert(conn, changed)

    repeated = get_price(conn, price_id)

    assert repeated["processed_at"] == updated["processed_at"]

    # 4. Older record: must not overwrite newer data.
    upsert(conn, original)

    final = get_price(conn, price_id)

    assert final["price"] == changed["price"]
    assert final["updated_at"] == changed["updated_at"]
    assert final["processed_at"] == updated["processed_at"]
