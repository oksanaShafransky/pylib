from airflow.operators.sensors import BaseSensorOperator
from airflow.models import DagBag
from airflow.bin import cli
from airflow.utils import apply_defaults, State
from airflow import settings, utils
import logging
from airflow.models import TaskInstance, Log


class AdaptedExternalTaskSensor(BaseSensorOperator):
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
    :param execution_delta: time difference with the previous execution to
        look at, the default is the same execution_date as the current task.
        For yesterday, use [positive!] datetime.timedelta(days=1)
    :type execution_delta: datetime.timedelta
    """

    template_fields = ('external_execution_date',)

    @apply_defaults
    def __init__(
            self,
            external_dag_id,
            external_task_id,
            allowed_states=None,
            external_execution_date=None,
            *args, **kwargs):
        super(AdaptedExternalTaskSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.external_execution_date = external_execution_date
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.pokes = 0

    def poke(self, context):
        if self.external_execution_date:
            dttm = self.external_execution_date
        else:
            dttm = context['execution_date']

        logging.info(
                'Poking for '
                '{self.external_dag_id}.'
                '{self.external_task_id} on '
                '{dttm} ... '.format(**locals()))
        TI = TaskInstance

        # Validate that the external dag and task exist once every 10 pokes
        if self.pokes % 10 == 0:
            logging.info('Validating the existing of the referenced task')
            dag_bag = DagBag(cli.DAGS_FOLDER)
            dag_bag.dags[self.external_dag_id].get_task(self.external_task_id)

        session = settings.Session()
        count = session.query(TI).filter(
                TI.dag_id == self.external_dag_id,
                TI.task_id == self.external_task_id,
                TI.state.in_(self.allowed_states),
                TI.execution_date == dttm,
                ).count()
        session.commit()
        session.close()
        self.pokes += 1
        return count
