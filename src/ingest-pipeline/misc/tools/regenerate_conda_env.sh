#!/bin/bash

#set -x  # for logging and debugging

# This specifies the instance of the environment to be created
if [ -z "$HUBMAP_INSTANCE" ]; then
    exit "The environment variable HUBMAP_INSTANCE is not set."
fi
instance="$HUBMAP_INSTANCE"

# What python version should be used?
python_version=3.9

# Root directory for newly created conda environments
conda_env_root="/opt/environments"

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

get_dir_of_this_script  # sets $DIR
cd $DIR

source source_platform_file.sh

echo $HM_AF_METHOD $HM_AF_ENV_NAME
if [ "${HM_AF_METHOD}" == 'conda' ] ; then
    which conda || export PATH=/opt/anaconda3/bin:$PATH
    eval "$(conda shell.bash hook)"
elif [ "${HM_AF_METHOD}" == 'module_conda' ] ; then
    source /etc/profile.d/modules.sh
    module use /hive/modulefiles
    module load anaconda
    eval "$(conda shell.bash hook)"
else
    echo "The config for this platform specifies a HM_AF_METHOD which is not one of 'conda' or 'module_conda'"
    exit -1
fi

# Use the existing environment if it can be found
conda_env_path=`conda env list | grep "${HM_AF_ENV_NAME}" | awk '{print $2}'`
if [ "${conda_env_path}" == "" ]; then
    if [ -e ${conda_env_root}/${HM_AF_ENV_NAME}/bin/python ]; then
	conda_env_path=${conda_env_root}/${HM_AF_ENV_NAME}
    fi
fi

# Create the environment if necessary
if [ "${conda_env_path}" == "" ]; then
    conda_env_path=${conda_env_root}/${HM_AF_ENV_NAME}
    echo 'Creating the conda environment'
    conda create --yes --prefix ${conda_env_path} python=${python_version} pip
else
    echo "Conda environment already exists"
fi

# Activate the environment
conda activate ${conda_env_path}

# Install requirements using pip
pip install -r ../../requirements.txt

