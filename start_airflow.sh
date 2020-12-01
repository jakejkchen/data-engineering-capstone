#!/bin/bash

export AIRFLOW_HOME=~/Documents/airflow

# Start airflow
airflow scheduler -D
airflow webserver -D  -p 8080

# Wait till airflow web-server is ready
echo "Waiting for Airflow web server..."
while true; do
  _RUNNING=$(ps aux | grep airflow-webserver | grep ready | wc -l)
  if [ $_RUNNING -eq 0 ]; then
    sleep 1
  else
    echo "Airflow web server is ready"
    break;
  fi
done
