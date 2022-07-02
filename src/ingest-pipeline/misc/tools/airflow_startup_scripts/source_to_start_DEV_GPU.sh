#!/bin/bash

      
logdir=$HOME/logs
#queue=general_prod,gpu000_q1_prod
queue=gpu000_q1_dev
host=`hostname`
name=celery-dev
n=8

nohup env HUBMAP_INSTANCE=dev \
      ./airflow_wrapper.sh worker --queues $queue --concurrency $n \
      --celery_hostname $name@$host \
      --stdout $logdir/$host.out --stderr $logdir/$host.err --log-file $logdir/$host.log
