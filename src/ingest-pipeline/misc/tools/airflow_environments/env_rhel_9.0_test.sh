#! /bin/bash

# HM_AF_METHOD must be one of venv, module_conda, or conda
# HM_AF_ENV_NAME must be the name of the conda environment or the full path to the venv dir
HM_AF_METHOD='conda'
HM_AF_ENV_NAME="condaEnv_rhel_9_python_${HUBMAP_PYTHON_VERSION}_test"

PARENTDIR="$(dirname "$(readlink -f "$0")")"
. "${PARENTDIR}/airflow_environments/env_test.sh"

HM_AF_CONFIG=/opt/repositories/vm004-prod/ingest-pipeline/src/ingest-pipeline/airflow/airflow.cfg
HM_AF_HOME=/opt/repositories/vm004-prod/ingest-pipeline/src/ingest-pipeline/airflow
HM_AF_CONN_INGEST_API_CONNECTION=http://vm004.hive.psc.edu:7777/
