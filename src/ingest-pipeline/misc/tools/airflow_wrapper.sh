#! /bin/bash

# set -x  # for logging and debugging

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


# Handle setting of environment variables.
#
# The goal is to let values from the environment (prefix HUBMAP_) override
# those from the config files (prefix HM_AF_).  We also check that all
# required variables are set at some level.
envvars=( CONFIG HOME \
	  CONN_INGEST_API_CONNECTION \
	  CONN_UUID_API_CONNECTION \
	  CONN_CELLS_API_CONNECTION \
	  CONN_SEARCH_API_CONNECTION \
	  CONN_ENTITY_API_CONNECTION \
	)
for varname in "${envvars[@]}" ; do
    full_varname="AIRFLOW_${varname}"
    cfg_varname="HM_AF_${varname}"
    if [[ -z "${!full_varname}" ]] ; then
	export ${full_varname}=${!cfg_varname}
    fi 
    if [[ -z "${!full_varname}" ]] ; then
	echo "${full_varname} is not set"
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
echo 'Environment follows'
printenv

cd $AIRFLOW_HOME ; \
env AIRFLOW__HUBMAP_API_PLUGIN__BUILD_NUMBER="$(cat ${top_level_dir}/build_number)" \
    airflow $*
