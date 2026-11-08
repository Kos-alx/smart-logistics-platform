
FROM apache/airflow:3.4.0

COPY --chown=airflow:0 docker/airflow/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/requirements.txt

COPY --chown=airflow:0 slp /opt/airflow/slp

ENV PYTHONPATH=/opt/airflow
