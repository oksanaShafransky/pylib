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
    'email': ['iddo.aviram@similarweb.com','felixv@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='MobileDailyPreliminary', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages

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


group_raw_files = DockerBashOperator(task_id='GroupRawDataByUser',
                                     dag=dag,
                                     docker_name='''{{ params.cluster }}''',
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/group_raw.sh -d {{ ds }} -p group -rt 2101 -rmem 1536'''
                                     )
group_raw_files.set_upstream(should_run)


merge_outliers = DockerBashOperator(task_id='MergeOutlierFiles',
                                    dag=dag,
                                    docker_name=DEFAULT_CLUSTER,
                                    bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/group_raw.sh -d {{ ds }} -p merge_outliers'''
                                    )
merge_outliers.set_upstream(group_raw_files)


report_outliers = DockerBashOperator(task_id='OutliersReport',
                                     dag=dag,
                                     docker_name=DEFAULT_CLUSTER,
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/group_raw.sh -d {{ ds }} -p outlier_report'''
                                     )
report_outliers.set_upstream(merge_outliers)


######################### Aggregation ##############################################

blocked_ips = DockerBashOperator(task_id='BlockedIPs',
                                 dag=dag,
                                 docker_name=DEFAULT_CLUSTER,
                                 bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/collection.sh -d {{ ds }} -p blocked -mmem 1536'''
                                 )
blocked_ips.set_upstream(group_raw_files)


detect_sysapps = DockerBashOperator(task_id='SystemAppDetection',
                                    dag=dag,
                                    docker_name=DEFAULT_CLUSTER,
                                    bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/collection.sh -d {{ ds }} -p sysapps -mmem 1536'''
                                    )
detect_sysapps.set_upstream(group_raw_files)


combine_sysapps = DockerBashOperator(task_id='CombineSystemApps',
                                     dag=dag,
                                     docker_name=DEFAULT_CLUSTER,
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/collection.sh -d {{ ds }} -p combine_sysapps'''
                                     )
combine_sysapps.set_upstream(detect_sysapps)


daily_agg = DockerBashOperator(task_id='DailyAggregation',
                               dag=dag,
                               docker_name=DEFAULT_CLUSTER,
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/collection.sh -d {{ ds }} -p aggregation -rt 637 -mmem 2560 -rmem 1536'''
                               )
daily_agg.set_upstream(blocked_ips)
daily_agg.set_upstream(combine_sysapps)


################## Wrap Up #########################

wrap_up = DummyOperator(task_id='FinishProcess', dag=dag)
wrap_up.set_upstream(daily_agg)

register_success = EtcdSetOperator(task_id='RegisterSuccessOnETCD',
                                   dag=dag,
                                   path='''services/mobile-stats/daily/{{ ds }}''',
                                   root=ETCD_ENV_ROOT['PRODUCTION']
                                   )
register_success.set_upstream(wrap_up)


# for now redundant, we may clean this data up, distinguishing it from mere success
register_available = EtcdSetOperator(task_id='SetDataAvailableDate',
                                     dag=dag,
                                     path='''services/mobile-stats/data-available/{{ ds }}''',
                                     root=ETCD_ENV_ROOT[dag_template_params['run_environment']]
                                     )
register_available.set_upstream(wrap_up)

