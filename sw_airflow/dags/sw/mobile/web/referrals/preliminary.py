__author__ = 'Amit Rom'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

from airflow.models import BaseOperator
from airflow.utils import apply_defaults
import time

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2015, 12, 1),
    'depends_on_past': False, # Amit fix
    'email': ['amitr@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

class TestKillOperator(BaseOperator):
    ui_color = '#00BFFF'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(TestKillOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        print "Sleeping..."
        time.sleep(60)

    def on_kill(self):
        print "Help, i'm being killed"

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='MobileWebReferralsDailyPreliminary', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


opera_raw_data_ready = EtcdSensor(task_id='OperaRawDataReady',
                                    dag=dag,
                                    root=ETCD_ENV_ROOT[dag_template_params['run_environment']],
                                    path='''services/opera-mini-s3/daily/{{ ds }}'''
)


test = TestKillOperator(task_id='TestKillOperator',
                 dag=dag)


filter_malformed_events = DockerBashOperator(task_id='FilterMalformedEvents',
                                     dag=dag,
                                     docker_name='''{{ params.cluster }}''',
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/preliminary.sh -d {{ ds }} -p filter_malformed_events -env main'''
)


filter_malformed_events.set_upstream(test)
filter_malformed_events.set_upstream(opera_raw_data_ready)


extract_invalid_users = DockerBashOperator(task_id='ExtractInvalidUsers',
                                    dag=dag,
                                    docker_name=DEFAULT_CLUSTER,
                                    bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/preliminary.sh -d {{ ds }} -p filter_users -env main'''
)
extract_invalid_users.set_upstream(filter_malformed_events)


filter_invalid_users = DockerBashOperator(task_id='FilterInvalidUsers',
                                     dag=dag,
                                     docker_name=DEFAULT_CLUSTER,
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/preliminary.sh -d {{ ds }} -p filter_invalid_users_from_events -env main'''
)
filter_invalid_users.set_upstream(extract_invalid_users)

mobile_web_referrals_preliminary = DummyOperator(task_id='MobileWebReferralsDailyPreliminary', dag=dag)
mobile_web_referrals_preliminary.set_upstream(filter_invalid_users)