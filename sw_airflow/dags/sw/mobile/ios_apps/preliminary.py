__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.sensors import HdfsSensor
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.key_value import *
from sw.airflow.docker_bash_operator import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/ios-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

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


icon_cache_resolution = \
    DockerBashOperator(task_id='IconCacheResolution',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''invoke -c {{ params.execution_dir }}/mobile/scripts/preliminary/ios/icon_cache_resolution icon_cache_resolution -d {{ ds }} -b {{ params.base_hdfs_dir}}''',
                       start_date=datetime(2016, 2, 9)
                       )
icon_cache_resolution.set_upstream(should_run)

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

sys_apps = DockerBashOperator(task_id='SystemAppDetection',
                              dag=dag,
                              docker_name=DEFAULT_CLUSTER,
                              bash_command='''invoke -c {{ params.execution_dir }}/mobile/scripts/preliminary/ios/daily_aggregation detect_user_apps -d {{ ds }} -b {{ params.base_hdfs_dir}}'''
                              )
sys_apps.set_upstream(ios_user_grouping)
sys_apps.set_upstream(map_ids)

user_apps_export = DockerBashOperator(task_id='ExportUserApps',
                                      dag=dag,
                                      docker_name=DEFAULT_CLUSTER,
                                      bash_command='''invoke -c {{ params.execution_dir }}/mobile/scripts/preliminary/ios/daily_aggregation export_user_apps -d {{ ds }} -b {{ params.base_hdfs_dir}}'''
                                      )
user_apps_export.set_upstream(sys_apps)


daily_aggregation = DockerBashOperator(task_id='DailyAggregation',
                                       dag=dag,
                                       docker_name=DEFAULT_CLUSTER,
                                       bash_command='''invoke -c {{ params.execution_dir }}/mobile/scripts/preliminary/ios/daily_aggregation daily_aggregation -d {{ ds }} -b {{ params.base_hdfs_dir}}'''
                                       )
daily_aggregation.set_upstream(ios_user_grouping)
daily_aggregation.set_upstream(map_ids)
daily_aggregation.set_upstream(user_apps_export)
daily_aggregation.set_upstream(export_sources)

preliminary = DummyOperator(task_id='Preliminary', dag=dag)
preliminary.set_upstream(daily_aggregation)

