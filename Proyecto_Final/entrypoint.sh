#!/bin/bash
# entrypoint.sh

export AIRFLOW_HOME=/opt/airflow

case "$1" in
  webserver)
    airflow db upgrade
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ]; then
      airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
    fi
    exec airflow webserver
    ;;
  scheduler)
    exec airflow scheduler
    ;;
  version)
    exec airflow version
    ;;
  *)
    exec "$@"
    ;;
esac
