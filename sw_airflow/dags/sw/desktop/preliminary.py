__author__ = 'Felix Vaisman'

from datetime import datetime, timedelta

from airflow.models import DAG

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(15, 11, 13),
    'depends_on_past': False,
    'email': ['bigdata@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='DesktopPreliminary', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages

should_run = CompoundDateEtcdSensor(task_id='RawDataReady',
                                    dag=dag,
                                    root=ETCD_ENV_ROOT[dag_template_params['run_environment']],
                                    key_list_path='services/copy_logs_daily/trackers/',
                                    list_separator=';',
                                    desired_date='''{{ ds }}''',
                                    key_root='services/data-ingestion/trackers/mrptracker',
                                    key_suffix='.sg.internal',
                                    execution_timeout=timedelta(minutes=240)
                                    )


group_raw = DockerBashOperator(task_id='GroupByUser',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''params.execution_dir/analytics/scripts/daily/preliminaryJobs.sh -d {{ ds }} -p group'''
                               )
group_raw.set_upstream(should_run)

blocked_ips = DockerBashOperator(task_id='BlockedIPs',
                                 dag=dag,
                                 docker_name='''{{ params.cluster }}''',
                                 bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/preliminaryJobs.sh -d {{ ds }} -p blocked_ips'''
                                 )
blocked_ips.set_upstream(group_raw)

daily_aggregation = DockerBashOperator(task_id='DailyAggregation',
                                       dag=dag,
                                       docker_name='''{{ params.cluster }}''',
                                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyAggregation.sh -d {{ ds }} -p aggregate'''
                                       )
daily_aggregation.set_upstream(group_raw)
daily_aggregation.set_upstream(blocked_ips)

repair_tables = DockerBashOperator(task_id='RepairDailyTables',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyAggregation.sh -d {{ ds }} -p repair'''
                                   )
repair_tables.set_upstream(daily_aggregation)

register_available = EtcdSetOperator(task_id='MarkDataAvailability',
                                     dag=dag,
                                     path='''services/aggregation/data-available/{{ ds }}''',
                                     root=ETCD_ENV_ROOT[dag_template_params['run_environment']]
                                     )
register_available.set_upstream(daily_aggregation)

