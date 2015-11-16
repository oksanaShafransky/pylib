__author__ = 'Felix Vaisman'

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
    'start_date': datetime(15, 11, 10),
    'depends_on_past': False,
    'email': ['felixv@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='RecruitDataService', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages

should_run_desktop = CompoundDateEtcdSensor(task_id='DesktopDataReady',
                                            dag=dag,
                                            root=ETCD_ENV_ROOT[dag_template_params['run_environment']],
                                            key_list_path='services/copy_logs_daily/trackers/',
                                            list_separator=';',
                                            desired_date='''{{ ds }}''',
                                            key_root='services/data-ingestion/trackers/mrptracker',
                                            key_suffix='.sg.internal',
                                            execution_timeout=timedelta(minutes=240)
                                            )

slice_desktop = DockerBashOperator(task_id='SliceDesktop',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/common/scripts/data-service/recruit.sh -d {{ ds }} -p slice_web -rmem 1536'''
                                   )
slice_desktop.set_upstream(should_run_desktop)

ship_desktop = DockerBashOperator(task_id='ShipDesktop',
                                  dag=dag,
                                  docker_name='''{{ params.cluster }}''',
                                  bash_command='''{{ params.execution_dir }}/common/scripts/data-service/recruit.sh -d {{ ds }} -p ship_web'''
                                  )
ship_desktop.set_upstream(slice_desktop)


should_run_mobile = CompoundDateEtcdSensor(task_id='MobileDataReady',
                                           dag=dag,
                                           root=ETCD_ENV_ROOT[dag_template_params['run_environment']],
                                           key_list_path='services/copy_logs_daily/trackers/',
                                           list_separator=';',
                                           desired_date='''{{ ds }}''',
                                           key_root='services/data-ingestion/trackers/mobile',
                                           key_suffix='.sg.internal',
                                           execution_timeout=timedelta(minutes=240)
                                           )


slice_mobile = DockerBashOperator(task_id='SliceMobile',
                                  dag=dag,
                                  docker_name='''{{ params.cluster }}''',
                                  bash_command='''{{ params.execution_dir }}/common/scripts/data-service/recruit.sh -d {{ ds }} -p slice_mobile_web -rmem 1536'''
                                  )
slice_mobile.set_upstream(should_run_mobile)

ship_mobile = DockerBashOperator(task_id='ShipMobile',
                                 dag=dag,
                                 docker_name='''{{ params.cluster }}''',
                                 bash_command='''{{ params.execution_dir }}/common/scripts/data-service/recruit.sh -d {{ ds }} -p ship_mobile_web'''
                                 )
ship_mobile.set_upstream(slice_mobile)