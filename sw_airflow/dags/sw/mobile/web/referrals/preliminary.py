from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta

from sw.airflow.airflow_etcd import *
from sw.airflow.docker_bash_operator import DockerBashOperatorFactory

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'MobileWeb',
    'start_date': datetime(2015, 12, 1),
    'depends_on_past': True,
    'email': ['amitr@similarweb.com,barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='MobileWeb_ReferralsPreliminary', default_args=dag_args, params=dag_template_params,
          schedule_interval=timedelta(days=1))

opera_raw_data_ready = EtcdSensor(task_id='opera_raw_data_ready',
                                  dag=dag,
                                  root=ETCD_ENV_ROOT[dag_template_params['run_environment']],
                                  path='''services/opera-mini-s3/daily/{{ ds }}'''
                                  )

factory = DockerBashOperatorFactory(use_defaults=True,
                                    dag=dag,
                                    script_path='''{{ params.execution_dir }}/mobile/scripts/web/referrals''',
                                    additional_cmd_components=['-env main'])

filter_malformed_events = factory.build(task_id='filter_malformed_events',
                                        core_command='preliminary.sh -p filter_malformed_events')
filter_malformed_events.set_upstream(opera_raw_data_ready)

extract_invalid_users = factory.build(task_id='extract_invalid_users',
                                      core_command='preliminary.sh -p filter_users ')
extract_invalid_users.set_upstream(filter_malformed_events)

filter_invalid_users = factory.build(task_id='filter_invalid_users',
                                     core_command='preliminary.sh -p filter_invalid_users_from_events''')
filter_invalid_users.set_upstream(extract_invalid_users)

referrals_preliminary = DummyOperator(task_id='ReferralsPreliminary', dag=dag)
referrals_preliminary.set_upstream(filter_invalid_users)
