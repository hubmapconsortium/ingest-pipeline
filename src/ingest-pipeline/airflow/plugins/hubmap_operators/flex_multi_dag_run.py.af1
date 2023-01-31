#! /usr/bin/env python

"""
This module is cloned with modifications from https://pypi.org/project/airflow-multi-dagrun/
https://raw.githubusercontent.com/mastak/airflow_multi_dagrun/master/airflow_multi_dagrun/operators/multi_dagrun.py
Author Ihor Liubymov infunt@gmail.com
Maintainer https://pypi.org/user/mastak/

The original iterates over multiple executions of the same trigger_dag_id; this version
allows that to change between iterations.
"""

from airflow import settings
from airflow.models import DagBag
# from airflow.operators.dagrun_operator import DagRunOrder
# from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
# from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.models import BaseOperator
import datetime as dt


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class FlexMultiDagRunOperator(BaseOperator):
    """
    Triggers zero or more DAG runs based on output of a generator

    :param conf: Configuration for the DAG run
    :type conf: dict
    :param execution_date: Execution date for the dag (templated)
    :type execution_date: str or datetime.datetime
    """

    template_fields = ("execution_date", "conf")
    ui_color = "#ffefeb"

    CREATED_DAGRUN_KEY = 'created_dagrun_key'

    # @apply_defaults
    def __init__(self, python_callable,
                 conf=None, execution_date=None,
                 op_args=None, op_kwargs=None,
                 provide_context=False, *args, **kwargs):
        super(FlexMultiDagRunOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.conf = conf
        if not isinstance(execution_date, (str, dt.datetime, type(None))):
            raise TypeError(
                "Expected str or datetime.datetime type for execution_date."
                "Got {}".format(type(execution_date))
            )
        self.execution_date = execution_date
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context

    def execute(self, context):
        if self.provide_context:
            context.update(self.op_kwargs)
            self.op_kwargs = context

        session = settings.Session()
        created_dr_ids = []
        for tuple in self.python_callable(*self.op_args, **self.op_kwargs):
            if not tuple:
                break
            trigger_dag_id, dro = tuple
            if not isinstance(dro, DagRunOrder):
                dro = DagRunOrder(payload=dro)

            now = dt.datetime.now(dt.timezone.utc)
            if dro.run_id is None:
                dro.run_id = 'trig__' + now.isoformat()

            dbag = DagBag(settings.DAGS_FOLDER)
            trigger_dag = dbag.get_dag(trigger_dag_id)
            dr = trigger_dag.create_dagrun(
                run_id=dro.run_id,
                execution_date=now,
                state=State.RUNNING,
                conf=dro.payload,
                external_trigger=True,
            )
            created_dr_ids.append(dr.id)
            self.log.info("Created DagRun %s, %s", dr, now)

        if created_dr_ids:
            session.commit()
            context['ti'].xcom_push(self.CREATED_DAGRUN_KEY, created_dr_ids)
        else:
            self.log.info("No DagRun created")
        session.close()