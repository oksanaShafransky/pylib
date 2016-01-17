__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.sensors import HdfsSensor

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/home/iddoa/similargroup_SIM-6508/feat/ios_reach_collection'
BASE_DIR = '/user/iddoa/ios_testing'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp-ios'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 1, 16),
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER,
                       'mode': 'window', 'mode_type': 'last-28'}

dag = DAG(dag_id='IosApps_Estimation', default_args=dag_args, params=dag_template_params,
          schedule_interval=timedelta(days=1))

reach_estimate = DockerBashOperator(task_id='ReachEstimate',
                                       dag=dag,
                                       docker_name=DEFAULT_CLUSTER,
                                       bash_command='''invoke  -c {{ params.execution_dir }}/mobile/scripts/app-engagement/ios/reach_estimate reach_estimate -d {{ ds }}'''
                                       )