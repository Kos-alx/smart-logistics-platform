"""Проверка одинаковой логики карточки маршрута в PostgreSQL и ClickHouse."""

import os
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[1]


def calculation(path):
    """Извлечь SELECT из DDL без отдельной копии расчёта для тестов."""
    return path.read_text().split("-- Начало расчёта\n", 1)[1].split(
        "-- Конец расчёта", 1
    )[0].strip()


def sample_rows():
    from slp.etl.clickhouse_prices_prepared import COLUMNS

    rows = []

    def add(route, month, prices, **changes):
        for index, price in enumerate(prices):
            row = dict.fromkeys(COLUMNS)
            row.update(
                price_id=len(rows) + 1, month=date.fromisoformat(month),
                first_price_date=date.fromisoformat(month), loading_date=date(2026, 10, 1),
                loading_city_gcid=route, unloading_city_gcid=100, origin_city="Отправление",
                destination_city="Назначение", distance=10., total_weight=20., total_volume=80.,
                weight_category="FTL", volume_category="FTL", body_group_id=56,
                body_group_name="Тент", price=float(price), currency_code="643",
                exchange_rate=1, price_rub=price, price_km_rub=float(price) / 10,
                organization_id=index % 5 + 1, subscriber_id=1, price_type=3,
                is_fact=True, complex=False, outlier_mark=False,
                processed_at=datetime(2026, 10, 26, tzinfo=UTC),
            )
            row.update(changes)
            rows.append(row)

    add(1, "2025-10-01", [100, 100])
    add(1, "2026-09-01", [100, 200])
    add(1, "2026-10-01", [200])
    add(1, "2026-10-01", [400], price_type=9, organization_id=2, volume_category="OTH")
    add(2, "2026-07-01", [100], weight_category=None)
    add(2, "2026-10-01", [200], weight_category=None)
    add(3, "2026-10-01", range(100, 130))
    add(4, "2026-09-01", range(100, 110))
    add(5, "2026-08-01", range(100, 130))
    add(6, "2026-10-01", range(100, 109))
    add(7, "2026-10-01", range(100, 130), price_type=1)
    for invalid in [{"is_fact": False}, {"price_type": 6}, {"complex": True},
                    {"outlier_mark": True}, {"price": 0}, {"distance": 0},
                    {"price_rub": 0}, {"first_price_date": date(2026, 10, 29)}]:
        add(8, "2026-10-01", [100], **invalid)
    add(8, "2026-11-01", [100])
    add(9, "2026-10-01", [100], loading_date=date(2027, 1, 1), organization_id=None)
    add(10, "2026-10-01", [100])
    add(10, "2026-10-01", [200], body_group_id=4, body_group_name="Рефрижератор")
    add(10, "2026-10-01", [300], weight_category="OTH")
    add(11, "2026-10-01", [100], first_price_date=date(2026, 10, 26))
    add(11, "2026-10-01", [200], first_price_date=date(2026, 10, 27))
    add(12, "2026-10-01", range(100, 129))
    for index in range(30):
        add(13, "2026-10-01", [100], organization_id=index % 4 + 1)
    add(14, "2026-09-01", range(100, 130))
    return rows


def assert_metrics(rows):
    cells = {(str(row["month"]), row["loading_city_gcid"]): row for row in rows}
    september = cells["2026-09-01", 1]
    assert september["p25_trip"] == pytest.approx(125)
    assert september["median_trip"] == pytest.approx(150)
    assert september["p75_trip"] == pytest.approx(175)
    assert september["avg_trip"] == pytest.approx(150)
    assert september["median_km"] == pytest.approx(15)
    assert september["cv_rate"] == pytest.approx(2 ** .5 / 3)
    october = cells["2026-10-01", 1]
    assert october["trips_count"] == october["carriers_count"] == 2
    assert october["mom_pct"] == pytest.approx(100)
    assert october["yoy_pct"] == pytest.approx(200)
    gap = cells["2026-10-01", 2]
    assert gap["mom_pct"] is None and gap["yoy_pct"] is None
    assert gap["cv_rate"] is None
    assert cells["2026-10-01", 3]["reliability_level"] == "HIGH"
    assert cells["2026-09-01", 4]["reliability_level"] == "MEDIUM"
    assert cells["2026-08-01", 5]["reliability_level"] == "MEDIUM"
    assert cells["2026-10-01", 6]["reliability_level"] == "LOW_DATA"
    assert cells["2026-10-01", 7]["carriers_count"] == 0
    assert cells["2026-10-01", 7]["reliability_level"] == "MEDIUM"
    assert all(key[1] != 8 for key in cells)
    assert cells["2026-10-01", 9]["carriers_count"] == 0
    grain_cells = [row for row in rows if row["loading_city_gcid"] == 10]
    assert len(grain_cells) == 3
    assert {(row["body_group_id"], row["weight_category"]) for row in grain_cells} == {
        (56, "FTL"), (4, "FTL"), (56, "OTH")
    }
    assert cells["2026-10-01", 11]["trips_count"] == 1
    assert cells["2026-10-01", 11]["median_trip"] == pytest.approx(100)
    assert cells["2026-10-01", 12]["reliability_level"] == "MEDIUM"
    assert cells["2026-10-01", 13]["carriers_count"] == 4
    assert cells["2026-10-01", 13]["reliability_level"] == "MEDIUM"
    assert cells["2026-09-01", 14]["reliability_level"] == "HIGH"


def test_route_card_files_exist():
    for path in (ROOT / "sql/mart/02_route_card.sql", ROOT / "sql/clickhouse/02_route_card.sql"):
        assert path.is_file(), f"Отсутствует SQL карточки: {path.name}"


def test_route_card_metrics_and_parity():
    if os.getenv("SLP_RUN_CH_TESTS") != "1":
        pytest.skip("Проверки PostgreSQL и ClickHouse отключены")
    from decimal import Decimal

    from slp.db import get_clickhouse_client, get_connection
    from slp.etl.clickhouse_prices_prepared import COLUMNS, ensure_tables

    conn = get_connection("SLP_CORE_DB_HOST")
    ch = get_clickhouse_client()
    table = "test_card_" + uuid4().hex
    try:
        conn.execute("CREATE TEMP TABLE card_input (LIKE mart.prices_prepared) ON COMMIT DROP")
        data = sample_rows()
        for row in data:
            row["price_rub"] = Decimal(row["price_rub"])
            row["exchange_rate"] = Decimal(row["exchange_rate"])
        values = [tuple(row[column] for column in COLUMNS) for row in data]
        with conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO card_input ({', '.join(COLUMNS)}) "
                f"VALUES ({', '.join(['%s'] * len(COLUMNS))})", values
            )
        query_pg = calculation(ROOT / "sql/mart/02_route_card.sql").replace(
            "mart.prices_prepared", "pg_temp.card_input"
        )
        with conn.cursor() as cur:
            cur.execute(query_pg)
            names = [col.name for col in cur.description]
            pg_rows = [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
        ensure_tables(ch, table)
        ch.insert(table, values, column_names=COLUMNS)
        query_ch = calculation(ROOT / "sql/clickhouse/02_route_card.sql").replace(
            "FROM prices_prepared", f"FROM {table}"
        )
        result = ch.query(query_ch)
        ch_rows = [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]
        assert_metrics(pg_rows)
        assert_metrics(ch_rows)
        key = lambda row: (row["month"], row["loading_city_gcid"], row["body_group_id"],
                           row["weight_category"] or "")
        assert len(pg_rows) == len(ch_rows)
        for pg_row, ch_row in zip(sorted(pg_rows, key=key), sorted(ch_rows, key=key), strict=True):
            assert pg_row.keys() == ch_row.keys()
            for name, value in pg_row.items():
                if isinstance(value, (float, Decimal)):
                    assert float(value) == pytest.approx(float(ch_row[name]), rel=1e-10), name
                else:
                    assert value == ch_row[name], name
    finally:
        conn.rollback()
        conn.close()
        ch.command(f"DROP TABLE IF EXISTS {table}_stage")
        ch.command(f"DROP TABLE IF EXISTS {table}")
        ch.close()
