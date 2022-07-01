#!/bin/bash

      
logdir=$HOME/bridges2
#queue=general_prod,gpu000_q1_prod
queue=gpu000_q1_prod
host=`hostname`
name=celery-prod
n=8

nohup env HUBMAP_INSTANCE=prod \
      ./airflow_wrapper.sh worker --queues $queue --concurrency $n \
      --celery_hostname $name@$host \
      --stdout $logdir/$host.out --stderr $logdir/$host.err --log-file $logdir/$host.log
