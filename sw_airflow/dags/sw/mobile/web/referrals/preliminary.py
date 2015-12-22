__author__ = 'Amit Rom'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(10, 11, 15),
    'depends_on_past': False,
    'email': ['amitr@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='MobileWebReferralsDailyPreliminary', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# TODO: define sensor for new opera raw data is ready
should_run = CompoundDateEtcdSensor(task_id='RawDataReady',
                                    dag=dag,
                                    root=ETCD_ENV_ROOT[dag_template_params['run_environment']],
                                    key_list_path='services/copy_logs_daily/trackers/',
                                    list_separator=';',
                                    desired_date='''{{ ds }}''',
                                    key_root='services/data-ingestion/trackers/mobile',
                                    key_suffix='.sg.internal',
                                    execution_timeout=timedelta(minutes=240)
)


filter_malformed_events = DockerBashOperator(task_id='FilterMalformedEvents',
                                     dag=dag,
                                     docker_name='''{{ params.cluster }}''',
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/preliminary.sh -d {{ ds }} -p filter_malformed_events -env main'''
)
filter_malformed_events.set_upstream(should_run)


extract_invalid_users = DockerBashOperator(task_id='ExtractInvalidUsers',
                                    dag=dag,
                                    docker_name=DEFAULT_CLUSTER,
                                    bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/preliminary.sh -d {{ ds }} -p filter_users -env main'''
)
extract_invalid_users.set_upstream(filter_malformed_events)


filter_invalid_users_from_events = DockerBashOperator(task_id='FilterInvalidUsers',
                                     dag=dag,
                                     docker_name=DEFAULT_CLUSTER,
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/preliminary.sh -d {{ ds }} -p filter_invalid_users_from_events -env main'''
)
filter_invalid_users_from_events.set_upstream(extract_invalid_users)