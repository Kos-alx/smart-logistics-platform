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

echo "=== Granting application full access ==="

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v slp_user="$SLP_DB_USER" <<'SQL'

GRANT ALL PRIVILEGES ON ALL TABLES
IN SCHEMA public TO :"slp_user";

GRANT ALL PRIVILEGES ON ALL SEQUENCES
IN SCHEMA public TO :"slp_user";

SQL

echo "=== Source initialization complete ==="
