#!/bin/bash

# This becomes the prompt of the created venv
label=dev

com_br=${COMMONS_BRANCH:-master}
export COMMONS_BRANCH=${com_br}

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

# set DIR to the directory of the current script
get_dir_of_this_script
cd $DIR

env_name=hubmap_${label}_env
echo 'The prompt for the new venv_b2 will be' \(${env_name}\)

module load anaconda3

if ( conda env list | grep ${env_name} )
then
    echo 'shell is' $SHELL
    if [[ ${CONDA_DEFAULT_ENV} == ${env_name} ]]
    then
	source deactivate
    fi

    echo "Asking permission to remove the old version of ${env_name}"
    if ! conda remove -n ${env_name} --all
    then
	exit -1
    fi
    
fi

export COMMONS_BRANCH=${com_br}
conda create --yes -n ${env_name} pip
source activate ${env_name}
pip install --upgrade pip
pip install -r ingest-pipeline/src/ingest-pipeline/requirements.txt



