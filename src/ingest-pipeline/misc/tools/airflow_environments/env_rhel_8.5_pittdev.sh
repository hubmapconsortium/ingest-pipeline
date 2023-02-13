#! /bin/bash

# HM_AF_METHOD must be one of venv, module_conda, or conda
# HM_AF_ENV_NAME must be the name of the conda environment or the full path to the venv dir
HM_AF_METHOD='conda'
HM_AF_ENV_NAME='condaEnv_rhel_8.5_python_3.6_dev'

PARENTDIR="$(dirname "$(readlink -f "$0")")"
. "${PARENTDIR}/airflow_environments/env_pittdev.sh"
