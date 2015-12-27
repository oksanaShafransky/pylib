from datetime import timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
WINDOW_MODE = 'window'
WINDOW_MODE_TYPE = 'last-28'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2015, 10, 1),
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=30)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER,
                       'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE}

dag = DAG(dag_id='FixDailyAppRanksBackfill', default_args=dag_args, params=dag_template_params,
       schedule_interval=timedelta(days=1))

suppl_eng = \
 DockerBashOperator(task_id='SupplEng',
                    dag=dag,
                    docker_name='''{{ params.cluster }}''',
                    bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/engagement.sh -d {{ yesterday_ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -env main -eo -p aggregate -f'''
                    )

suppl_ranks = \
 DockerBashOperator(task_id='SupplRanks',
                    dag=dag,
                    docker_name='''{{ params.cluster }}''',
                    bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/ranks.sh -d {{ yesterday_ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -env main -p join_scores_info,cat_ranks -f'''
                    )

suppl_ranks.set_upstream(suppl_eng)

fix_daily_app_ranks_backfill = \
 DummyOperator(task_id='FixDailyAppRanksBackfill',
               dag=dag
               )

fix_daily_app_ranks_backfill.set_upstream(suppl_ranks)