__author__ = 'Felix Vaisman'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.key_value import *
from sw.airflow.docker_bash_operator import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 1, 14),
    'depends_on_past': False,
    'email': ['felixv@similarweb.com','iddoav@similarweb.com', 'barakg@similarweb.com','amitr@similarweb.com>'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='Mobile_Preliminary', default_args=dag_args, params=dag_template_params, schedule_interval="@daily")


# define stages

should_run = KeyValueCompoundDateSensor(task_id='RawDataReady',
                                        dag=dag,
                                        env='PRODUCTION',
                                        key_list_path='services/copy_logs_daily/trackers',
                                        list_separator=';',
                                        desired_date='''{{ ds }}''',
                                        key_root='services/data-ingestion/trackers/mobile',
                                        key_suffix='.sg.internal',
                                        execution_timeout=timedelta(minutes=240)
                                        )


group_raw_data_by_user = DockerBashOperator(task_id='GroupRawDataByUser',
                                            dag=dag,
                                            docker_name='''{{ params.cluster }}''',
                                            bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/group_raw.sh -d {{ ds }} -p group -rt 2101 -rmem 1536'''
                                            )
group_raw_data_by_user.set_upstream(should_run)


merge_outlier_files = DockerBashOperator(task_id='MergeOutlierFiles',
                                         dag=dag,
                                         docker_name=DEFAULT_CLUSTER,
                                         bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/group_raw.sh -d {{ ds }} -p merge_outliers'''
                                         )
merge_outlier_files.set_upstream(group_raw_data_by_user)


outliers_report = DockerBashOperator(task_id='OutliersReport',
                                     dag=dag,
                                     docker_name=DEFAULT_CLUSTER,
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/group_raw.sh -d {{ ds }} -p outlier_report'''
                                     )
outliers_report.set_upstream(merge_outlier_files)


######################### Aggregation ##############################################

blocked_ips = DockerBashOperator(task_id='BlockedIPs',
                                 dag=dag,
                                 docker_name=DEFAULT_CLUSTER,
                                 bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/collection.sh -d {{ ds }} -p blocked -mmem 1536'''
                                 )
blocked_ips.set_upstream(group_raw_data_by_user)


system_app_detection = DockerBashOperator(task_id='SystemAppDetection',
                                          dag=dag,
                                          docker_name=DEFAULT_CLUSTER,
                                          bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/collection.sh -d {{ ds }} -p sysapps -mmem 1536'''
                                          )
system_app_detection.set_upstream(group_raw_data_by_user)


combine_system_apps = DockerBashOperator(task_id='CombineSystemApps',
                                         dag=dag,
                                         docker_name=DEFAULT_CLUSTER,
                                         bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/collection.sh -d {{ ds }} -p combine_sysapps'''
                                         )
combine_system_apps.set_upstream(system_app_detection)


daily_aggregation = DockerBashOperator(task_id='DailyAggregation',
                                       dag=dag,
                                       docker_name=DEFAULT_CLUSTER,
                                       bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/collection.sh -d {{ ds }} -p aggregation -rt 1201 -mmem 2560 -rmem 1536'''
                                       )
daily_aggregation.set_upstream(blocked_ips)
daily_aggregation.set_upstream(combine_system_apps)


################## Wrap Up #########################

register_success_on_etcd = KeyValueSetOperator(task_id='RegisterSuccessOnETCD',
                                               dag=dag,
                                               path='''services/mobile-stats/daily/{{ ds }}''',
                                               env='PRODUCTION'
                                               )
register_success_on_etcd.set_upstream(daily_aggregation)


# for now redundant, we may clean this data up, distinguishing it from mere success
set_data_available_date = KeyValueSetOperator(task_id='SetDataAvailableDate',
                                              dag=dag,
                                              path='''services/mobile-stats/data-available/{{ ds }}''',
                                              env='''{{ params.run_environment }}'''
                                              )
set_data_available_date.set_upstream(daily_aggregation)

preliminary = DummyOperator(task_id='Preliminary', dag=dag)
preliminary.set_upstream([register_success_on_etcd, set_data_available_date])

