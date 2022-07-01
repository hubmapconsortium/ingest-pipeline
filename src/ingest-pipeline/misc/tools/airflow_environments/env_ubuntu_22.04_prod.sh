#! /bin/bash

# HM_AF_METHOD must be one of venv, module_conda, or conda
# HM_AF_ENV_NAME must be the name of the conda environment or the full path to the venv dir
HM_AF_METHOD='conda'
HM_AF_ENV_NAME='hubmapEnv'

#set airflow environment
#HM_AF_CONFIG=/hive/users/hive/hubmap/hivevm193-prod/ingest-pipeline/src/ingest-pipeline/airflow/airflow.cfg
HM_AF_CONFIG=/home/welling/git/hubmap/ingest-pipeline/src/ingest-pipeline/airflow/airflow.cfg
#HM_AF_HOME=/hive/users/hive/hubmap/hivevm193-prod/ingest-pipeline/src/ingest-pipeline/airflow
HM_AF_HOME=/home/welling/git/hubmap/ingest-pipeline/src/ingest-pipeline/airflow

HM_AF_CONN_INGEST_API_CONNECTION=http://hivevm193.psc.edu:7777/
HM_AF_CONN_UUID_API_CONNECTION=http://https%3a%2f%2fuuid.api.hubmapconsortium.org/
HM_AF_CONN_CELLS_API_CONNECTION=http://https%3a%2f%2fcells.api.hubmapconsortium.org/
HM_AF_CONN_SEARCH_API_CONNECTION=http://https%3a%2f%2fsearch.api.hubmapconsortium.org/
HM_AF_CONN_ENTITY_API_CONNECTION=http://https%3a%2f%2fentity.api.hubmapconsortium.org/



