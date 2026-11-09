#!/usr/bin/env bash
set -Eeuo pipefail

# Выполняем SQL инициализации в базе, созданной entrypoint ClickHouse.
for file in /opt/slp/sql/clickhouse/*.sql; do
    clickhouse-client \
        --user "${CLICKHOUSE_USER:?}" \
        --password "${CLICKHOUSE_PASSWORD:?}" \
        --database "${CLICKHOUSE_DB:-slp}" \
        --multiquery < "$file"
done
