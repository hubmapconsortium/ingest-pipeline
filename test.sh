#!/usr/bin/env bash

# Accepts additional args to pass to pytest
# Accepts flags as individual args as well as
# custom "--test=" arg which can be used to run a specific
# test file/class/test just as you would pass it to unittest directly.
# Example:
# ./test.sh "--test=tests/test_status_changer.py::TestEntityUpdater::test_get_entity_type" --pdb

set -o errexit


PYTHONPATH=src/ingest-pipeline/airflow/dags/
env AIRFLOW_HOME=${PWD}/src/ingest-pipeline/airflow PYTHONPATH=$PYTHONPATH python -m unittest tests.test_status_changer tests.test_timetables
