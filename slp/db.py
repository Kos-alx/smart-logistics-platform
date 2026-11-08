
"""Общие подключения к PostgreSQL."""

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
