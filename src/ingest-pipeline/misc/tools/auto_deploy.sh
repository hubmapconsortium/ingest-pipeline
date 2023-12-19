#!/bin/bash

# What variables do we need?
# Define HIVE machines
#hive_machines=("l001.hive.psc.edu" "gpu002.pvt.hive.psc.edu")
hive_machines=()
#b2_machines=("v004.pvt.bridges2.psc.edu")
b2_machines=()
repo_env="dev"
repo_dir="/opt/repositories/$(hostname -s)-$repo_env/ingest-pipeline"

regenerate_env=false

while getopts ":g" opt; do
  case $opt in
    g)
      regenerate_env=true
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

set -x
for machine in ${hive_machines[@]}; do
       	# Rsync repo to machine
        rsync -a --exclude "src/ingest-pipeline/airflow/logs" $repo_dir/ $machine:$repo_dir

       	# If flag set, run the conda environment regenerations
        if $regenerate_env ; then
                ssh $machine "/usr/local/bin/update_hubmap.sh $repo_dir $repo_env"
        fi
done

# Separate because its easier to loop twice over a small list than insert string checking and manipulation
for machine in ${b2_machines[@]}; do
        # Rsync repo to machine
        rsync -a --exclude "src/ingest-pipeline/airflow/logs" 'ssh -J bridges2.psc.edu' $repo_dir/ $machine:$repo_dir

        # If flag set, run the conda environment regenerations
        if $regenerate_env ; then
                ssh -J "bridges2.psc.edu" $machine "/usr/local/bin/update_hubmap.sh $repo_dir $repo_env"
        fi
done
