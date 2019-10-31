#!/bin/bash -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"  # thank you stackoverflow
BUILD_NUM=`cat ${DIR}/../../../build_number`
TZ=`cat /etc/timezone`
run_id=`docker run -d -p 8080:8080 \
	-v $PWD/dags:/usr/local/airflow/dags \
	-v $PWD/plugins/:/usr/local/airflow/plugins \
	-v $PWD/data/:/usr/local/airflow/data \
	-e AIRFLOW__HUBMAP_API_PLUGIN__HUBMAP_API_AUTH=1234 \
	-e AIRFLOW__HUBMAP_API_PLUGIN__BUILD_NUMBER=$BUILD_NUM \
	-e TZ=$TZ \
	puckel/docker-airflow webserver | cut -c1-12`
run_name=`docker ps --format "{{.ID}} {{.Names}}" | grep ${run_id} | awk '{print $2}'`
echo $run_name
