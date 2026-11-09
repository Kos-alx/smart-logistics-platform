
#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== Инициализация SLP: Core, DQ, Meta и Mart ==="

: "${POSTGRES_DB:?}"
: "${POSTGRES_USER:?}"
: "${SLP_DB_USER:?}"

# Создание таблиц и других объектов Core, DQ и Meta.
for file in /opt/slp/sql/core/*.sql; do
    echo "Применение SQL: $file"

    psql -v ON_ERROR_STOP=1 \
      --username "$POSTGRES_USER" \
      --dbname "$POSTGRES_DB" \
      -v slp_user="$SLP_DB_USER" \
      -f "$file"
done

# Создание объектов аналитического слоя Mart.
for file in /opt/slp/sql/mart/*.sql; do
    echo "Применение SQL: $file"

    psql -v ON_ERROR_STOP=1 \
      --username "$POSTGRES_USER" \
      --dbname "$POSTGRES_DB" \
      -v slp_user="$SLP_DB_USER" \
      -f "$file"
done

# Предоставление пользователю приложения прав на все слои.
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v slp_user="$SLP_DB_USER" <<'SQL'

GRANT ALL PRIVILEGES ON SCHEMA
    core, dq, meta_data, mart TO :"slp_user";

GRANT ALL PRIVILEGES ON ALL TABLES
IN SCHEMA core, dq, meta_data, mart TO :"slp_user";

GRANT ALL PRIVILEGES ON ALL SEQUENCES
IN SCHEMA core, dq, meta_data, mart TO :"slp_user";

SQL

echo "=== Инициализация Core, DQ, Meta и Mart завершена ==="
