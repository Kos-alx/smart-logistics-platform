#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== Applying source schema ==="

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -f /opt/slp/sql/source/schema.sql

echo "=== Generating synthetic data ==="

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v n_prices="${SLP_N_PRICES:-300000}" \
  -f /opt/slp/sql/source/synthetic.sql

echo "=== Granting source read permissions ==="

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v slp_user="$SLP_DB_USER" <<'SQL'
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"slp_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO :"slp_user";
SQL

echo "=== Source initialization complete ==="
