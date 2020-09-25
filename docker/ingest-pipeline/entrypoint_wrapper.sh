#!/usr/bin/env bash

# This is cloned and modified from 
# https://github.com/swapniel99/docker-airflow/raw/cb2fb21c92e19501d4fcea265d940e0fab484f30/script/entrypoint_wrapper.sh
#

# Install custom python package if requirements.txt is present
if [[ -e "/requirements.txt" ]]; then
    $(command -v pip) install --user -r /requirements.txt
fi

# Global defaults and back-compat
export AIRFLOW__CORE__FERNET_KEY=`python -c 'from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)'`

case "$1" in
  webserver|worker|flower)
    # Give the scheduler time to run upgradedb.
    sleep 10
    exec /entrypoint "$@"
    ;;
  scheduler)
    echo "Attempting upgradedb command.."
    # In upgradedb default connections are not populated. Use "airflow initdb" instead for default connections.
    airflow upgradedb
    if [[ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ]] || [[ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]];
    then
      # Running webserver in scheduler instead of reverse to maintain consistency in Makefile.
      # With the "Local" and "Sequential" executors it should all run in one container.
      airflow webserver &
    fi
    exec /entrypoint "$@"
    ;;
  bash|python)
    exec /entrypoint "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec /entrypoint "$@"
    ;;
esac
