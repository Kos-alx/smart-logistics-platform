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

# Create application roles
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

# Create Airflow database
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


# Grant full application access to deals_bigdata
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v source_db="$POSTGRES_DB" \
  -v slp_user="$SLP_DB_USER" <<'SQL'

ALTER DATABASE :"source_db" OWNER TO :"slp_user";

GRANT ALL PRIVILEGES ON DATABASE :"source_db" TO :"slp_user";

GRANT ALL PRIVILEGES ON SCHEMA public TO :"slp_user";

ALTER DEFAULT PRIVILEGES FOR ROLE postgres
GRANT ALL PRIVILEGES ON TABLES TO :"slp_user";

ALTER DEFAULT PRIVILEGES FOR ROLE postgres
GRANT ALL PRIVILEGES ON SEQUENCES TO :"slp_user";

SQL

echo "=== PostgreSQL bootstrap complete ==="
