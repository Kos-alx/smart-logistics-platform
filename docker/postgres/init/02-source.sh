
#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== Инициализация схемы источника данных ==="

# Создание таблиц источника.
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -f /opt/slp/sql/source/schema.sql

echo "=== Генерация синтетических данных ==="

# Наполнение источника тестовыми данными.
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v n_prices="${SLP_N_PRICES:-300000}" \
  -f /opt/slp/sql/source/synthetic.sql

echo "=== Выдача прав пользователю приложения ==="

# Предоставление доступа к таблицам и последовательностям источника.
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v slp_user="$SLP_DB_USER" <<'SQL'

GRANT ALL PRIVILEGES ON ALL TABLES
IN SCHEMA public TO :"slp_user";

GRANT ALL PRIVILEGES ON ALL SEQUENCES
IN SCHEMA public TO :"slp_user";

SQL

echo "=== Инициализация источника данных завершена ==="
