#!/bin/bash

# This becomes the prompt of the created venv
label=dev

commons_branch=${COMMONS_BRANCH:-master}

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
if [ -e venv ] ; then
  if [ -e venv_old ] ; then
    echo 'Please remove or rename venv_old so I can save the current venv under that name'
    exit 1
  fi
  mv venv venv_old
fi

echo 'The prompt for the new venv will be' \(${label}\)

scl enable rh-python36 bash <<EOF

  export COMMONS_BRANCH=${commons_branch}
  virtualenv --prompt=\(${label}\) venv
  . venv/bin/activate
  pip install 'apache-airflow[celery,crypto,postgres,redis,ssh]'
  pip install -r ingest-pipeline/src/ingest-pipeline/requirements.txt

EOF

