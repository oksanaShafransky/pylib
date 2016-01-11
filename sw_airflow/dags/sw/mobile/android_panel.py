__author__ = 'Felix Vaisman'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.sensors import HdfsSensor

from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(10, 11, 15),
    'depends_on_past': False,
    'email': ['bigdata@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='AndroidDailyPanelReport', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages

group_files_base = '/similargroup/data/stats-mobile/raw'
should_run = HdfsSensor(task_id='GroupFilesReady',
                        dag=dag,
                        hdfs_conn_id='hdfs_%s' % DEFAULT_CLUSTER,
                        filepath='''%s/{{ macros.date_partition(ds) }}/_SUCCESS''' % group_files_base,
                        execution_timeout=timedelta(minutes=240)
                        )


panel_stats = DockerBashOperator(task_id='PanelStats',
                                 dag=dag,
                                 docker_name='''{{ params.cluster }}''',
                                 bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/panel.sh -d {{ ds }} -p stats -mmem 2048'''
                                 )
panel_stats.set_upstream(should_run)

panel_users = DockerBashOperator(task_id='CollectUsers',
                                 dag=dag,
                                 docker_name='''{{ params.cluster }}''',
                                 bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/panel.sh -d {{ ds }} -p collect_users -mmem 1536'''
                                 )
panel_users.set_upstream(should_run)


agg_users = DockerBashOperator(task_id='AggregateUsers',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/panel.sh -d {{ ds }} -p users_agg -mmem 1536'''
                               )
agg_users.set_upstream(panel_users)


churn = DockerBashOperator(task_id='UserChurn',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/panel.sh -d {{ ds }} -p churn -mmem 1536'''
                           )
churn.set_upstream(panel_users)

store = DockerBashOperator(task_id='StoreSql',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/panel.sh -d {{ ds }} -p report'''
                           )
store.set_upstream(panel_stats)
store.set_upstream(agg_users)
store.set_upstream(churn)



