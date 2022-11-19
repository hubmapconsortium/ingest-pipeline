#! /bin/bash

# Path to top level directory of repo
if [ -z "${top_level_dir}" ]; then
   top_level_dir="$(git rev-parse --show-toplevel)"
fi

# Path from top level to the airflow environments directory
airflow_env_rel_path=src/ingest-pipeline/misc/tools/airflow_environments


source /etc/os-release
platform_string="${ID}_${VERSION_ID}_${instance}"
platform_file="${top_level_dir}/${airflow_env_rel_path}/env_${platform_string}.sh"
if [[ -e "${platform_file}" ]] ; then
    source "${platform_file}"
else
    echo "Platform configuration file ${platform_file} does not exist"
    exit -1
fi

