from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(23, 11, 15),
    'depends_on_past': False,
    'email': ['bigdata@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='HdfsBackupAndRetentionDag', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages

retention_targets = [
    ['/similargroup/data/raw-stats-ios', 230],
    ['/similargroup/data/raw-stats-japan', 1000]
    ]

for entry in retention_targets:
    path, retention_period = entry

    backup_and_retention_op = \
        DockerBashOperator(task_id='HdfsBackupAndRetentionTask',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''python {{ params.execution_dir }}/utils/scripts/backup_and_retention.py --path %s --retention_days %s --log_level %s --dryrun %s''' % (path, retention_period, 'DEBUG', 'True')
                           )



