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
    'start_date': datetime(2015, 12, 7),
    'depends_on_past': False,
    'email': ['bigdata@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER,
                       'mode': 'window', 'mode_type': 'last-28'}

dag = DAG(dag_id='iOSPreliminary', default_args=dag_args, params=dag_template_params,
          schedule_interval=timedelta(days=1))

ios_user_grouping = \
    DockerBashOperator(task_id='IosUserGrouping',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''invoke  -c {{ params.execution_dir }}/mobile/scripts/preliminary/ios/user_grouping user_grouping -d {{ ds }}'''
                       )

daily_aggregation = DockerBashOperator(task_id='DailyAggregation',
                                       dag=dag,
                                       docker_name=DEFAULT_CLUSTER,
                                       bash_command='''invoke  -c {{ params.execution_dir }}/mobile/scripts/preliminary/ios/daily_aggregation daily_aggregation -d {{ ds }}'''
                                       )
#daily_aggregation.set_upstream(ios_user_grouping)
