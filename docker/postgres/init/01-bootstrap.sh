#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== SLP PostgreSQL bootstrap ==="

: "${POSTGRES_USER:?}"
: "${POSTGRES_DB:?}"
: "${SLP_DB_USER:?}"
: "${SLP_DB_PASSWORD:?}"
: "${AIRFLOW_DB:?}"
: "${AIRFLOW_DB_USER:?}"
: "${AIRFLOW_DB_PASSWORD:?}"

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname postgres \
  -v slp_user="$SLP_DB_USER" \
  -v slp_password="$SLP_DB_PASSWORD" \
  -v airflow_user="$AIRFLOW_DB_USER" \
  -v airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE ROLE :"slp_user" LOGIN PASSWORD :'slp_password';
CREATE ROLE :"airflow_user" LOGIN PASSWORD :'airflow_password';
SQL

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname postgres \
  -v airflow_db="$AIRFLOW_DB" \
  -v airflow_user="$AIRFLOW_DB_USER" <<'SQL'
SELECT format(
  'CREATE DATABASE %I OWNER %I',
  :'airflow_db',
  :'airflow_user'
) \gexec
SQL

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v source_db="$POSTGRES_DB" \
  -v slp_user="$SLP_DB_USER" <<'SQL'
CREATE SCHEMA raw;
CREATE SCHEMA core;
CREATE SCHEMA mart;
CREATE SCHEMA ops;

GRANT CONNECT ON DATABASE :"source_db" TO :"slp_user";
GRANT USAGE ON SCHEMA public TO :"slp_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"slp_user";

GRANT USAGE, CREATE ON SCHEMA raw, core, mart, ops TO :"slp_user";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw, core, mart, ops TO :"slp_user";
SQL

echo "=== PostgreSQL bootstrap complete ==="
