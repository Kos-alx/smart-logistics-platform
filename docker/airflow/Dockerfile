
FROM apache/airflow:3.4.0-python3.12

COPY --chown=airflow:0 docker/airflow/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/requirements.txt

COPY --chown=airflow:0 slp /opt/airflow/slp
COPY --chown=airflow:0 sql/queries /opt/airflow/sql/queries
COPY --chown=airflow:0 sql/clickhouse /opt/airflow/sql/clickhouse
COPY --chown=airflow:0 sql/mart /opt/airflow/sql/mart
COPY --chown=airflow:0 docker/airflow/prepare-logs.sh /opt/airflow/prepare-logs.sh

# Именованный volume наследует права каталога; группа 0 доступна AIRFLOW_UID.
RUN mkdir -p /opt/airflow/artifacts/ml && chmod -R g+rwX /opt/airflow/artifacts

ENV PYTHONPATH=/opt/airflow
