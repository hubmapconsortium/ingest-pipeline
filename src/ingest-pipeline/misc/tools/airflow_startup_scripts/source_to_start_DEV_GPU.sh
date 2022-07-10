#!/bin/bash
#
# Source this file from the parent directory containing airflow_wrapper.sh
#
instance=dev
logdir=$HOME/logs
#queue=general_${instance},gpu000_q1_${instance}
queue=gpu000_q1_${instance}
host=`hostname`
name=celery-${instance}
n=8
logfile=${logdir}/${host}_${instance}
nohup_file=${logdir}/nohup_${host}_${instance}.out

nohup env HUBMAP_INSTANCE=${instance} \
      ./airflow_wrapper.sh worker --queues $queue --concurrency $n \
      --celery_hostname $name@$host \
      --stdout ${logfile}.out --stderr ${logfile}.err --log-file ${logfile}.log > ${nohup_file}
