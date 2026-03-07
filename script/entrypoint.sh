#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  pip install --user -r /opt/airflow/requirements.txt
fi

# When using Postgres, airflow.db doesn't exist; ensure DB is initialized and admin user exists
airflow db upgrade
airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin \
  2>/dev/null || true

exec airflow webserver