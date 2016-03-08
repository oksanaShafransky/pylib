__author__ = 'Felix Vaisman'

from datetime import datetime, timedelta

from airflow.models import DAG

from sw.airflow.key_value import *
from sw.airflow.docker_bash_operator import DockerBashOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_EXECUTION_DIR = '/similargroup/adv_trk'
BASE_DIR = '/similargroup/data/advanced-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2015, 11, 1),
    'depends_on_past': False,
    'email': ['andrews@similarweb.com', 'kfire@similarweb.com', 'n7i6d2a2m1h2l3f6@similar.slack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=60)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER, 'tracker_type': 'mrpadvtracker'}

dag = DAG(dag_id='Advanced_Preliminary', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages

should_run = KeyValueCompoundDateSensor(task_id='RawDataReady',
                                        dag=dag,
                                        env='PRODUCTION',
                                        key_list_path='services/copy_logs_daily/trackers',
                                        list_separator=';',
                                        desired_date='''{{ ds }}''',
                                        key_root='''services/data-ingestion/trackers/{{ params.tracker_type }}''',
                                        key_suffix='.sg.internal',
                                        execution_timeout=timedelta(minutes=240)
                                        )


group_raw = DockerBashOperator(task_id='GroupByUser',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/preliminaryJobs.sh -s {{ ds }} -e {{ ds }} -p group -src advanced -cd {{ params.base_hdfs_dir }}'''
                               )
group_raw.set_upstream(should_run)

blocked_ips = DockerBashOperator(task_id='BlockedIPs',
                                 dag=dag,
                                 docker_name='''{{ params.cluster }}''',
                                 bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/preliminaryJobs.sh -s {{ ds }} -e {{ ds }} -p blocked_ips,sources -src simple,advanced -cd {{ params.base_hdfs_dir }}'''
                                 )
blocked_ips.set_upstream(group_raw)

daily_visit_detection = DockerBashOperator(task_id='DailyVisitDetection',
                                           dag=dag,
                                           docker_name='''{{ params.cluster }}''',
                                           bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyAggregation.sh -d {{ ds }} -p create_visits -cd {{ params.base_hdfs_dir }}'''
                                           )
daily_visit_detection.set_upstream(blocked_ips)

daily_aggregation = DockerBashOperator(task_id='DailyAggregation',
                                       dag=dag,
                                       docker_name='''{{ params.cluster }}''',
                                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyAggregation.sh -d {{ ds }} -p aggregate -cd {{ params.base_hdfs_dir }}'''
                                       )
daily_aggregation.set_upstream(daily_visit_detection)

repair_tables = DockerBashOperator(task_id='RepairDailyTables',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyAggregation.sh -d {{ ds }} -p repair -cd {{ params.base_hdfs_dir }} -hdb advanced'''
                                   )
repair_tables.set_upstream(daily_aggregation)

register_available = KeyValueSetOperator(task_id='MarkDataAvailability',
                                         dag=dag,
                                         path='''services/advanced-stats-aggregation/data-available/{{ ds }}''',
                                         env='''{{ params.run_environment }}'''
                                         )
register_available.set_upstream(daily_aggregation)

###########
# Wrap-up #
###########

wrap_up = \
    DummyOperator(task_id='Preliminary',
                  dag=dag
                  )
wrap_up.set_upstream([repair_tables, register_available])

