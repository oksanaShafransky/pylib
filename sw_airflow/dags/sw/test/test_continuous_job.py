__author__ = 'Felix Vaisman'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators import TimeDeltaSensor, TriggerDagRunOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 3, 3),
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=2)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='TestContinuousJob', default_args=dag_args, params=dag_template_params, schedule_interval=None)


# define stages

job_mock = TimeDeltaSensor(
        task_id='JobMock',
        delta = timedelta(minutes=1),
        dag=dag)


def new_trigger_condition(context, dag_run_obj):
    return dag_run_obj

trigger_new_run = TriggerDagRunOperator(
        task_id='TiggerNewRun',
        dag_id='TestContinuousJob',
        python_callable=new_trigger_condition,
        dag=dag)

trigger_new_run.set_upstream(job_mock)