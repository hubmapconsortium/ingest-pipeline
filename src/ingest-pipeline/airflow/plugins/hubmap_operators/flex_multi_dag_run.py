#! /usr/bin/env python


"""
This module is cloned with modifications from https://pypi.org/project/airflow-multi-dagrun/
https://github.com/mastak/airflow_multi_dagrun/blob/master/airflow_multi_dagrun/operators.py
Author Ihor Liubymov infunt@gmail.com
Maintainer https://pypi.org/user/mastak/

The original iterates over multiple executions of the same trigger_dag_id; this version
allows that to change between iterations.
"""

import typing as t

from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.models import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import datetime as dt
from airflow.utils.operator_helpers import determine_kwargs
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType

from airflow_multi_dagrun.utils import get_multi_dag_run_xcom_key


class FlexMultiDagRunOperator(TriggerDagRunOperator):

    def __init__(self, op_args=None, op_kwargs=None, python_callable=None, *args, **kwargs):
        super(FlexMultiDagRunOperator, self).__init__(*args, **kwargs)
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.python_callable = python_callable

    @provide_session
    def execute(self, context: t.Dict, session=None):
        context.update(self.op_kwargs)
        self.op_kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        created_dr_ids = []
        for tuple in self.python_callable(*self.op_args, **self.op_kwargs):
            if not tuple:
                break

            execution_date = dt.datetime.now(dt.timezone.utc)
            self.trigger_dag_id, conf = tuple

            run_id = conf.get('run_id')
            if not run_id:
                run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date)

            dag_run = DagRun.find(dag_id=self.trigger_dag_id, run_id=run_id)
            if not dag_run:
                dag_run = trigger_dag(
                    dag_id=self.trigger_dag_id,
                    run_id=run_id,
                    conf=conf,
                    execution_date=execution_date,
                    replace_microseconds=False,
                )
                self.log.info("Created DagRun %s, %s - %s", dag_run, self.trigger_dag_id, run_id)
            else:
                dag_run = dag_run[0]
                self.log.warning("Fetched existed DagRun %s, %s - %s", dag_run, self.trigger_dag_id, run_id)

            created_dr_ids.append(dag_run.id)

        if created_dr_ids:
            xcom_key = get_multi_dag_run_xcom_key(context['execution_date'])
            context['ti'].xcom_push(xcom_key, created_dr_ids)
            self.log.info("Pushed %s DagRun's ids with key %s", len(created_dr_ids), xcom_key)
        else:
            self.log.info("No DagRuns created")
