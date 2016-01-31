__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.sensors import HdfsSensor
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.key_value import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/ios-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp-ios'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2015, 11, 1),
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER,
                       'mode': 'window', 'mode_type': 'last-28'}

dag = DAG(dag_id='IosApps_Preliminary', default_args=dag_args, params=dag_template_params,
          schedule_interval=timedelta(days=1))

should_run = KeyValueCompoundDateSensor(task_id='RawDataReady',
                                        dag=dag,
                                        env='PRODUCTION',
                                        key_list_path='services/copy_logs_daily/trackers',
                                        list_separator=';',
                                        desired_date='''{{ ds }}''',
                                        key_root='services/data-ingestion/trackers/ios',
                                        key_suffix='.sg.internal',
                                        execution_timeout=timedelta(minutes=240)
                                        )

ios_user_grouping = \
    DockerBashOperator(task_id='IosUserGrouping',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''invoke -c {{ params.execution_dir }}/mobile/scripts/preliminary/ios/user_grouping user_grouping -d {{ ds }} -b {{ params.base_hdfs_dir}}'''
                       )
ios_user_grouping.set_upstream(should_run)

export_sources = DockerBashOperator(task_id='ExportSourcesForAnalyze',
                                    dag=dag,
                                    docker_name=DEFAULT_CLUSTER,
                                    bash_command='''invoke -c {{ params.execution_dir }}/mobile/scripts/preliminary/ios/daily_aggregation export_sources_for_analyze -d {{ ds }} -b {{ params.base_hdfs_dir}}'''
                                    )

map_ids = DockerBashOperator(task_id='ExportAppIDs',
                             dag=dag,
                             docker_name=DEFAULT_CLUSTER,
                             bash_command='''invoke -c {{ params.execution_dir }}/mobile/scripts/preliminary/ios/daily_aggregation export_app_id_mapping -d {{ ds }} -b {{ params.base_hdfs_dir}}'''
                             )


daily_aggregation = DockerBashOperator(task_id='DailyAggregation',
                                       dag=dag,
                                       docker_name=DEFAULT_CLUSTER,
                                       bash_command='''invoke -c {{ params.execution_dir }}/mobile/scripts/preliminary/ios/daily_aggregation daily_aggregation -d {{ ds }} -b {{ params.base_hdfs_dir}}'''
                                       )
daily_aggregation.set_upstream(ios_user_grouping)
daily_aggregation.set_upstream(map_ids)

preliminary = DummyOperator(task_id='Preliminary', dag=dag)
preliminary.set_upstream(daily_aggregation)
