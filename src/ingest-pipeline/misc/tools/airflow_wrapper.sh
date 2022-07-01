#! /bin/bash

set -x

# allowed values of HUBMAP_INSTANCE
hubmap_instance_strings=" prod test dev proto stage "

# Path from top level to the airflow environments directory
airflow_env_rel_path=src/ingest-pipeline/misc/tools/airflow_environments


# function to find the path to this script
function get_dir_of_this_script () {
    # This function sets DIR to the directory in which this script itself is found.
    # Thank you https://stackoverflow.com/questions/59895/how-to-get-the-source-directory-of-a-bash-script-from-within-the-script-itself
    SCRIPT_SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SCRIPT_SOURCE" ]; do # resolve $SCRIPT_SOURCE until the file is no longer a symlink
	DIR="$( cd -P "$( dirname "$SCRIPT_SOURCE" )" >/dev/null 2>&1 && pwd )"
	SCRIPT_SOURCE="$(readlink "$SCRIPT_SOURCE")"
	# if $SCRIPT_SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
	[[ $SCRIPT_SOURCE != /* ]] && SCRIPT_SOURCE="$DIR/$SCRIPT_SOURCE" 
    done
    DIR="$( cd -P "$( dirname "$SCRIPT_SOURCE" )" >/dev/null 2>&1 && pwd )"
    }

# function to check for presence of token $2 in list $1
contains() {
    [[ $1 =~ (^|[[:space:]])$2($|[[:space:]]) ]] && echo 1 || echo 0
}


# check for instance info
if [[ -z "${HUBMAP_INSTANCE}" ]] ; then
    echo "HUBMAP_INSTANCE is not defined"
    exit -1
else
    instance="${HUBMAP_INSTANCE}"
fi
if [ $(contains "${hubmap_instance_strings}" "${instance}") == 0 ] ; then
   echo "${instance} is not one of ${hubmap_instance_strings}"
   exit -1
fi


# set DIR to the directory of the current script, and find the source tree top level
get_dir_of_this_script
cd "$DIR"
top_level_dir="$(git rev-parse --show-toplevel)"

# establish OS context
source /etc/os-release
platform_string="${ID}_${VERSION_ID}_${instance}"
platform_file="${top_level_dir}/${airflow_env_rel_path}/env_${platform_string}.sh"
if [[ -e "${platform_file}" ]] ; then
    source "${platform_file}"
else
    echo "Platform configuration file ${platform_file} does not exist"
    exit -1
fi

echo "Check for required variables here!"
envvars=( AIRFLOW_CONFIG \
	      AIRFLOW_HOME \
	      AIRFLOW_CONN_INGEST_API_CONNECTION \
	      AIRFLOW_CONN_UUID_API_CONNECTION \
	      AIRFLOW_CONN_CELLS_API_CONNECTION \
	      AIRFLOW_CONN_SEARCH_API_CONNECTION \
	      AIRFLOW_CONN_ENTITY_API_CONNECTION \
	)
for varname in "${envvars[@]}" ; do
    echo ${!varname}
    if [[ -z "${!varname}" ]] ; then
	echo "${varname} is not set"
	exit -1
    fi 
done

if [ "${HM_AF_METHOD}" == 'conda' ] ; then
    eval "$(conda shell.bash hook)"
    conda activate "${HM_AF_ENV_NAME}"
elif [ "${HM_AF_METHOD}" == 'module_conda' ] ; then
    module load anaconda3
    eval "$(conda shell.bash hook)"
    conda activate "${HM_AF_ENV_NAME}"
elif [ "${HM_AF_METHOD}" == 'venv' ] ; then
    source "${HM_AF_ENV_NAME}/bin/activate"
else
    echo "unknown HM_AF_METHOD ${HM_AF_METHOD}"
    exit -1
fi
echo 'PATH follows'
echo $PATH
echo 'PYTHONPATH follows'
echo $PYTHONPATH


cd $AIRFLOW_HOME ; \
env AIRFLOW__HUBMAP_API_PLUGIN__BUILD_NUMBER="$(cat ${top_level_dir}/build_number)" \
    echo ./airflow_exe $*
