__author__ = 'Kfir Eittan'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 1, 100),
    'depends_on_past': True,
    'email': ['kfire@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='DesktopDailyEstimation', default_args=dag_args, params=dag_template_params,
          schedule_interval=timedelta(days=1))


desktop_daily_preliminary = ExternalTaskSensor(external_dag_id='DesktopPreliminary',
                                               external_task_id='DesktopPreliminary',
                                              dag=dag,
                                              task_id="DesktopPreliminary")
#########################
# estimation
#########################

estimation = \
    DockerBashOperator(task_id='Estimation',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                       )

estimation.set_upstream(desktop_daily_preliminary)

check = \
    DockerBashOperator(task_id='Check',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/qa/checkSiteAndCountryEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -nw 7'''
                       )

check.set_upstream(estimation)

###########
# Wrap-up #
###########

wrap_up = \
    DummyOperator(task_id='DesktopDailyEstimation',
                  dag=dag
                  )
wrap_up.set_upstream(check)
