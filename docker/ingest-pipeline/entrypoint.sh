#!/usr/bin/env bash

# This is cloned and modified from 
# https://github.com/swapniel99/docker-airflow/raw/cb2fb21c92e19501d4fcea265d940e0fab484f30/script/entrypoint_wrapper.sh
#

#export PATH=/bin:/usr/local/bin:/usr/bin:/home/airflow/.local/bin:$PATH
export PATH=/home/airflow/.local/bin:$PATH

# Install custom python package if requirements.txt is present
pip install --upgrade pip
pip install --user flask-admin
pip install --user 'apache-airflow[celery,crypto,postgres,redis,ssh]<2.0.0'
if [[ -e "/requirements.txt" ]]; then
    $(command -v pip) install --user -r /requirements.txt
fi

# Global defaults and back-compat
export AIRFLOW__CORE__FERNET_KEY=`python -c 'from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)'`
export AIRFLOW__WEBSERVER__SECRET_KEY=`openssl rand -hex 30`

# Load DAGs examples (default: Yes)
if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]; then
  AIRFLOW__CORE__LOAD_EXAMPLES=False
fi
export AIRFLOW__CORE__LOAD_EXAMPLES


case "$1" in
  webserver)
    airflow initdb
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; then
      # With the "Local" and "Sequential" executors it should all run in one container.
      airflow scheduler &
    fi
    exec airflow webserver
    ;;
  worker|scheduler)
    # Give the webserver time to run initdb.
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac

