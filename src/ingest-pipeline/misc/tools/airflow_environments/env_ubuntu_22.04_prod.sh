#! /bin/bash

# HM_AF_METHOD must be one of venv, module_conda, or conda
# HM_AF_ENV_NAME must be the name of the conda environment or the full path to the venv dir
HM_AF_METHOD='conda'
HM_AF_ENV_NAME='hubmapEnv'

#set airflow environment
#export AIRFLOW_CONFIG=/hive/users/hive/hubmap/hivevm193-prod/ingest-pipeline/src/ingest-pipeline/airflow/airflow.cfg
export AIRFLOW_CONFIG=/home/welling/git/hubmap/ingest-pipeline/src/ingest-pipeline/airflow/airflow.cfg
#export AIRFLOW_HOME=/hive/users/hive/hubmap/hivevm193-prod/ingest-pipeline/src/ingest-pipeline/airflow
#export AIRFLOW_HOME=/hive/users/hive/hubmap/hivevm193-prod/ingest-pipeline/src/ingest-pipeline/airflow
export AIRFLOW_HOME=/home/welling/git/hubmap/ingest-pipeline/src/ingest-pipeline/airflow

export AIRFLOW_CONN_INGEST_API_CONNECTION=http://hivevm193.psc.edu:7777/
export AIRFLOW_CONN_UUID_API_CONNECTION=https://uuid.api.hubmapconsortium.org/
export AIRFLOW_CONN_CELLS_API_CONNECTION=https://cells.api.hubmapconsortium.org/
export AIRFLOW_CONN_SEARCH_API_CONNECTION=https://search.api.hubmapconsortium.org/
#export AIRFLOW_CONN_ENTITY_API_CONNECTION=https://entity.api.hubmapconsortium.org/
export AIRFLOW_CONN_ENTITY_API_CONNECTION=http://https%3a%2f%2fentity.api.hubmapconsortium.org/



