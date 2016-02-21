import calendar
import logging

from airflow import settings
from airflow.bin import cli
from airflow.models import DagBag
from airflow.models import TaskInstance
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults, State, AirflowException
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
            dates_to_query=None,
            task_existence_check_interval=timedelta(minutes=10),
            *args, **kwargs):
        super(BaseExternalTaskSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.task_existence_check_interval = task_existence_check_interval
        self.dates_to_query = dates_to_query

        self.last_check_date = datetime.now()

    def poke(self, context):
        return self.internal_poke(self.dates_to_query)

    def internal_poke(self, dates_to_query):
        logging.info('Poking for '
                     '{self.external_dag_id}.'
                     '{self.external_task_id} on '
                     '{dates_to_query} ... '.format(**locals()))

        # Validate that the external dag and task exist every task_existence_check_interval
        if datetime.now() - self.last_check_date > self.task_existence_check_interval:
            logging.info('Validating the existence of the referenced task:')
            dag_bag = DagBag(cli.DAGS_FOLDER)
            dag_bag.dags[self.external_dag_id].get_task(self.external_task_id)
            logging.info('The referenced task was validated and found to be ok')

        TI = TaskInstance
        session = settings.Session()
        count = session.query(TI).filter(
                TI.dag_id == self.external_dag_id,
                TI.task_id == self.external_task_id,
                TI.state.in_(self.allowed_states),
                TI.execution_date.in_(dates_to_query)).count()
        session.commit()
        session.close()
        if count >= len(dates_to_query):
            return count
        return 0


class AdaptedExternalTaskSensor(BaseExternalTaskSensor):
    """
    default implementation of BaseExternalTaskSensor
    """

    @apply_defaults
    def __init__(self, external_execution_date=None, *args, **kwargs):
        super(AdaptedExternalTaskSensor, self).__init__(*args, **kwargs)
        self.external_execution_date = external_execution_date

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
    def __init__(self, execution_delta=0, *args, **kwargs):
        super(DeltaExternalTaskSensor, self).__init__(*args, **kwargs)
        self.execution_delta = execution_delta

    def poke(self, context):
        return super.internal_poke([(context['execution_date'] - self.execution_delta)])


class AggRangeExternalTaskSensor(BaseExternalTaskSensor):
    """
    see BaseExternalTaskSensor
    """

    @apply_defaults
    def __init__(self, agg_mode='last-28', *args, **kwargs):
        super(AggRangeExternalTaskSensor, self).__init__(*args, **kwargs)
        if agg_mode != 'monthly' and not agg_mode.startswith('last'):
            raise AirflowException('AggRangeExternalTaskSensor: unsupported agg_mode=%s' % self.agg_mode)
        self.agg_mode = agg_mode

    def poke(self, context):
        dt = datetime.date(context['execution_date'])
        if self.agg_mode == 'monthly':
            days_in_month = calendar.monthrange(dt.year, dt.month)[1]
            dates_to_query = macros.get_days(days_in_month, days_in_month)
        elif self.agg_mode.startswith('last'):
            num_days_in_range = self.agg_mode.split('-')[1]
            dates_to_query = macros.get_days(dt, num_days_in_range)

        return super.internal_poke(dates_to_query)