
FROM apache/airflow:3.4.0

COPY --chown=airflow:0 docker/airflow/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/requirements.txt

COPY --chown=airflow:0 slp /opt/airflow/slp
COPY --chown=airflow:0 sql/queries /opt/airflow/sql/queries
COPY --chown=airflow:0 sql/clickhouse /opt/airflow/sql/clickhouse

ENV PYTHONPATH=/opt/airflow
