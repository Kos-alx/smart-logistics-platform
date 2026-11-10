#!/usr/bin/env bash
set -Eeuo pipefail

# Все рабочие сервисы Airflow запускаются с группой 0 независимо от AIRFLOW_UID.
# Подготавливаем только общий volume логов, не затрагивая БД и ML-артефакты.
log_directory=/opt/airflow/logs
mkdir -p "$log_directory"
chgrp -R 0 "$log_directory"
chmod -R g+rwX "$log_directory"

# Новые подкаталоги наследуют общую группу, даже если владелец имеет другой UID.
find "$log_directory" -type d -exec chmod g+s {} +

echo "Volume логов Airflow готов к записи для группы 0."
