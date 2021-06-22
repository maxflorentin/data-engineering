# -*- coding: utf-8 -*-

import os

from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException, AirflowRescheduleException
from airflow.models import TaskInstance, DagBag, DagModel, DagRun
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.utils import timezone
from time import sleep
from airflow.models.taskreschedule import TaskReschedule
from datetime import datetime, timedelta
from sqlalchemy import String, func, distinct
from sqlalchemy.sql import func as f


class ZondaDagSensor(BaseSensorOperator):
    """
    Waits for a different DAG or a task in a different DAG to complete for a
    specific execution_date

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: str
    :param external_task_id: The task_id that contains the task you want to
        wait for. If ``None`` the sensor waits for the DAG
    :type external_task_id: str
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param execution_date: time difference with the previous execution to
        look at, the default is the same execution_date as the current task or DAG.
        For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to
        ExternalTaskSensor, but not both.
    :type execution_date: date
    :param check_existence: Set to `True` to check if the external task exists (when
        external_task_id is not None) or check if the DAG to wait for exists (when
        external_task_id is None), and immediately cease waiting if the external task
        or DAG does not exist (default value: False).
    :type check_existence: bool
    """
    template_fields = ['external_dag_id', 'external_task_id', 'execution_date', 'timeout']
    ui_color = '#19647e'

    @apply_defaults
    def __init__(self,
                 external_dag_id,
                 external_task_id=None,
                 allowed_states=None,
                 execution_date=None,
                 check_existence=False,
                 timeout=60 * 60 * 24,
                 *args,
                 **kwargs):
        super(ZondaDagSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        if not set(self.allowed_states) <= set(State.dag_states):
            raise ValueError(
                'Valid values for `allowed_states` '
                'when `external_task_id` is `None`: {}'.format(State.dag_states)
            )

        if execution_date is None:
            raise ValueError(
                'execution_date may '
                'be provided to ZondaDagSensor.')

        self.execution_date = execution_date
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.check_existence = check_existence
        self.timeout = timeout

    def execute(self, context):
        started_at = timezone.utcnow()
        if self.reschedule:
            # If reschedule, use first start date of current try
            task_reschedules = TaskReschedule.find_for_task_instance(context['ti'])
            if task_reschedules:
                started_at = task_reschedules[0].start_date
        while not self.poke(context):
            if (timezone.utcnow() - started_at).total_seconds() > self.timeout:
                # If sensor is in soft fail mode but will be retried then
                # give it a chance and fail with timeout.
                # This gives the ability to set up non-blocking AND soft-fail sensors.
                if self.soft_fail and not context['ti'].is_eligible_to_retry():
                    self._do_skip_downstream_tasks(context)
                    raise AirflowSkipException('Snap. Time is OUT.')
                else:
                    raise AirflowSensorTimeout('Snap. Time is OUT.')
            if self.reschedule:
                reschedule_date = timezone.utcnow() + timedelta(
                    seconds=self.poke_interval)
                raise AirflowRescheduleException(reschedule_date)
            else:
                sleep(self.poke_interval)

        self.log.info("Success criteria met")

    @provide_session
    def poke(self, context, session=None):

        dttm_filter = [self.execution_date]

        ready_to_start = []

        if isinstance(self.external_dag_id, dict):

            for dag_id, task_id in self.external_dag_id.items():

                self.log.info(
                    'Poking for %s.%s on %s ... ',
                    dag_id, task_id, self.execution_date
                )

                DM = DagModel
                TI = TaskInstance
                DR = DagRun

                if self.check_existence:
                    dag_to_wait = session.query(DM).filter(
                        DM.dag_id == self.external_dag_id
                    ).first()

                    if not dag_to_wait:
                        raise AirflowException('The external DAG '
                                               '{} does not exist.'.format(self.external_dag_id))
                    else:
                        if not os.path.exists(dag_to_wait.fileloc):
                            raise AirflowException('The external DAG '
                                                   '{} was deleted.'.format(self.external_dag_id))

                    if task_id != 'all':
                        refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(dag_id)
                        if not refreshed_dag_info.has_task(task_id):
                            raise AirflowException('The external task'
                                                   '{} in DAG {} does not exist.'.format(task_id, dag_id))

                if task_id != 'all':

                    task_ids = [task.strip() for task in task_id.split(',')]

                    count = session.query(func.count(distinct(TI.task_id))).filter(
                        TI.dag_id == dag_id,
                        TI.task_id.in_(task_ids),
                        TI.state.in_(self.allowed_states),
                        f.substr(func.cast(TI.execution_date, String), 1, 10) == self.execution_date,
                    ).scalar()
                    ready_to_start.append(count == len(task_ids))

                else:

                    count = session.query(func.count(distinct(DR.dag_id))).filter(
                        DR.dag_id == dag_id,
                        DR.state.in_(self.allowed_states),
                        func.hex(DR.conf) == self.execution_date.encode('utf-8').hex(),
                    ).scalar()
                    ready_to_start.append(count == len(dttm_filter))

                session.commit()
        return all(ready_to_start)
