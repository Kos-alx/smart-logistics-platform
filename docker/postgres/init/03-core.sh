#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== Initializing SLP Core and DQ ==="

: "${POSTGRES_DB:?}"
: "${SLP_DB_USER:?}"

for file in /opt/slp/sql/core/*.sql; do
    echo "Applying: $file"

    psql -v ON_ERROR_STOP=1 \
      --username "$POSTGRES_USER" \
      --dbname "$POSTGRES_DB" \
      -v slp_user="$SLP_DB_USER" \
      -f "$file"
done

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v slp_user="$SLP_DB_USER" <<'SQL'

GRANT ALL PRIVILEGES ON SCHEMA
    core, dq, meta_data TO :"slp_user";

GRANT ALL PRIVILEGES ON ALL TABLES
IN SCHEMA core, dq, meta_data TO :"slp_user";

GRANT ALL PRIVILEGES ON ALL SEQUENCES
IN SCHEMA core, dq, meta_data TO :"slp_user";

SQL
echo "=== Core and DQ initialization complete ==="