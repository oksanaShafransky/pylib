__author__ = 'Felix Vaisman'

from datetime import datetime, timedelta

from airflow.models import DAG

from sw.airflow.airflow_etcd import *

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime.now() - timedelta(days=1),
    'depends_on_past': False,
    'email': ['felixv@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='MobileDailyPreliminary', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define jobs

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
