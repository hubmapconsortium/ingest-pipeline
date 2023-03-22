#! /bin/bash

# HM_AF_METHOD must be one of venv, module_conda, or conda
# HM_AF_ENV_NAME must be the name of the conda environment or the full path to the venv dir
# HM_AF_METHOD='module_conda'
HM_AF_METHOD='conda'
HM_AF_ENV_NAME='condaEnv_centos_7_python_3.6_dev'
# HM_AF_METHOD='venv'
# HM_AF_ENV_NAME='/hive/users/hive/hubmap/hivevm191-dev/venv'

PARENTDIR="$(dirname "$(readlink -f "$0")")"
. "${PARENTDIR}/airflow_environments/env_pittdev.sh"
