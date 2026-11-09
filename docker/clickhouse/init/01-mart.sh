#!/usr/bin/env bash
set -Eeuo pipefail

# Выполняем SQL инициализации в базе, созданной entrypoint ClickHouse.
clickhouse-client \
    --user "${CLICKHOUSE_USER:?}" \
    --password "${CLICKHOUSE_PASSWORD:?}" \
    --database "${CLICKHOUSE_DB:-slp}" \
    --multiquery \
    < /opt/slp/sql/clickhouse/01_prices_prepared.sql
