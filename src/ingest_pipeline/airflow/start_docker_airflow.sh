#!/bin/bash -x

run_id=`docker run -d -p 8080:8080 \
	-v $PWD/dags:/usr/local/airflow/dags \
	-v $PWD/plugins/:/usr/local/airflow/plugins \
	-e EXPLAIN_TEMPLATE_LOADING=TRUE \
	puckel/docker-airflow webserver | cut -c1-12`
run_name=`docker ps --format "{{.ID}} {{.Names}}" | grep ${run_id} | awk '{print $2}'`
echo $run_name
