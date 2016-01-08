__author__ = 'Felix Vaisman'

from datetime import datetime, timedelta

from airflow.models import DAG

from sw.airflow.key_value import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(15, 11, 12),
    'depends_on_past': False,
    'email': ['bigdata@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='iOSDailyPanelReport', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages

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


user_report = DockerBashOperator(task_id='CollectUserData',
                                 dag=dag,
                                 docker_name='''{{ params.cluster }}''',
                                 bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/ios.sh -s {{ ds }} -e {{ ds }} -p collect_user_data'''
                                 )
user_report.set_upstream(should_run)

construct_report = DockerBashOperator(task_id='BuildReport',
                                      dag=dag,
                                      docker_name='''{{ params.cluster }}''',
                                      bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/ios.sh -s {{ ds }} -e {{ ds }} -p daily_stats_report,merge_users_table,daily_activity_report,daily_joined_report'''
                                      )
construct_report.set_upstream(user_report)


store = DockerBashOperator(task_id='StoreSql',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/ios.sh -s {{ ds }} -e {{ ds }} -p reports_to_sql'''
                           )
store.set_upstream(construct_report)


notify = DockerBashOperator(task_id='NotifyParties',
                            dag=dag,
                            docker_name='''{{ params.cluster }}''',
                            bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/ios.sh -s {{ ds }} -e {{ ds }} -p mail'''
                            )
notify.set_upstream(store)

