
"""Подключения к PostgreSQL и ClickHouse."""

import os

import psycopg


def get_connection(host_env: str) -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ[host_env],
        port=int(os.getenv("SLP_DB_PORT", "5432")),
        dbname=os.environ["SLP_DB_NAME"],
        user=os.environ["SLP_DB_USER"],
        password=os.environ["SLP_DB_PASSWORD"],
    )


def get_clickhouse_client():
    """Открыть HTTP-клиент ClickHouse с настройками из окружения."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.getenv("SLP_CH_HOST", "clickhouse"),
        port=int(os.getenv("SLP_CH_PORT", "8123")),
        database=os.getenv("CLICKHOUSE_DB", "slp"),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )
