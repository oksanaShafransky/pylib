import logging

from airflow import settings
from airflow.bin import cli
from airflow.models import DagBag
from airflow.models import TaskInstance
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults, State
from datetime import timedelta, datetime


class BaseExternalTaskSensor(BaseSensorOperator):
    """
    Waits for a task to complete in a different DAG

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: string
    :param external_task_id: The task_id that contains the task you want to
        wait for
    :type external_task_id: string
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param external_execution_date the date on which to poke the external task's
        run. The default is the same execution_date as the current task.
    :type: datetime.datetime
    :param execution_delta: time offset to look at,
        For yesterday, use [positive!] datetime.timedelta(days=1). Default is no offset.
        If both external_execution_date and execution_delta are given, the result is the
        offset of external_execution_date.timedelta(days=execution_delta).
    :type execution_delta: datetime.timedelta
    """

    @apply_defaults
    def __init__(
            self,
            external_dag_id,
            external_task_id,
            allowed_states=None,
            days_range=None,
            task_existence_check_interval=timedelta(minutes=10),
            *args, **kwargs):
        super(BaseExternalTaskSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.task_existence_check_interval = task_existence_check_interval
        self.days_range = days_range

        self.last_check_date = datetime.now()
        self.expected_num_results = 1 if not days_range else len(days_range)

    def poke(self, context):
        if self.days_range:
            dates_to_query = self.days_range
        else:
            dates_to_query = [context['execution_date']]

        return self.internal_poke(dates_to_query)

    def internal_poke(self, dates_to_query):
        logging.info(
                'Poking for '
                '{self.external_dag_id}.'
                '{self.external_task_id} on '
                '{dttm} ... '.format(**locals()))
        TI = TaskInstance
        # Validate that the external dag and task exist every task_existence_check_interval
        if datetime.now() - self.last_check_date > self.task_existence_check_interval:
            logging.info('Validating the existence of the referenced task:')
            dag_bag = DagBag(cli.DAGS_FOLDER)
            dag_bag.dags[self.external_dag_id].get_task(self.external_task_id)
            logging.info('The referenced task was validated and found to be ok')
        session = settings.Session()
        count = session.query(TI).filter(
                TI.dag_id == self.external_dag_id,
                TI.task_id == self.external_task_id,
                TI.state.in_(self.allowed_states),
                TI.execution_date.in_(dates_to_query)).count()
        session.commit()
        session.close()
        if count >= self.expected_num_results:
            return count
        return 0


class AdaptedExternalTaskSensor(BaseExternalTaskSensor):
    """
    see BaseExternalTaskSensor
    """

    @apply_defaults
    def __init__(
            self,
            external_dag_id,
            external_task_id,
            allowed_states=None,
            external_execution_date=None,
            task_existence_check_interval=timedelta(minutes=10),
            *args, **kwargs):
        super(AdaptedExternalTaskSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.external_execution_date = external_execution_date
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.task_existence_check_interval = task_existence_check_interval
        self.last_check_date = datetime.now()

    def poke(self, context):
        if self.external_execution_date:
            dttm = self.external_execution_date
        else:
            dttm = context['execution_date']

        return super.internal_poke([dttm])


class DeltaExternalTaskSensor(BaseExternalTaskSensor):
    """
    see BaseExternalTaskSensor
    """

    @apply_defaults
    def __init__(
            self,
            external_dag_id,
            external_task_id,
            allowed_states=None,
            execution_delta=None,
            *args, **kwargs):
        super(DeltaExternalTaskSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.execution_delta = execution_delta
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id

    def poke(self, context):
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        else:
            dttm = context['execution_date']

        return super.internal_poke([dttm])


class RangeExternalTaskSensor(BaseExternalTaskSensor):
    """
    see BaseExternalTaskSensor
    """

    @apply_defaults
    def __init__(
            self,
            external_dag_id,
            external_task_id,
            end_date,
            days_in_range,
            allowed_states=None,
            days_range=None,
            task_existence_check_interval=timedelta(minutes=10),
            *args, **kwargs):
        super(RangeExternalTaskSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.days_range = days_range

    def poke(self, context):
        if self.days_range:
            dates_to_query = self.days_range
        else:
            dates_to_query = [context['execution_date']]

        return self.internal_poke(dates_to_query)
