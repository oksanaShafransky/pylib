__author__ = 'Felix Vaisman'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators import BashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(20, 1, 16),
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='Test123', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages

test1 = BashOperator(
        task_id='print_force_1',
        bash_command='{% if task_instance.force %} echo force {% else %} echo no_force {% endif %}',
        dag=dag)

test2 = BashOperator(
        task_id='print_force_2',
        bash_command='{% if task_instance.force %} echo force {% else %} echo no_force {% endif %}',
        dag=dag)

test2.set_upstream(test1)