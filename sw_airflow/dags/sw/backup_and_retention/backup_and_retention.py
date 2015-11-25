from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
DOCKER_IMAGE = 'mrp-retention'

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
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER, 'docker_image': DOCKER_IMAGE}

dag = DAG(dag_id='HdfsBackupAndRetentionDag', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages

# list of: path, number of retention days, is backup required (if false, we just delete)
retention_targets = [
    ['/similargroup/data/analytics/daily/estimation/temp-sqs-output', 1, False],
    ['/similargroup/data/analytics/daily/post-estimate/estimate=intermediate-values', 1, False],
    ['/similargroup/data/analytics/daily/post-estimate/estimate=ratios/', 1, False]
    ]

for i in range(0, len(retention_targets)):
    path, retention_period, backup_required = retention_targets[i]

    backup_and_retention_op = \
        DockerBashOperator(task_id='HdfsBackupAndRetentionTask-%s' % (i),
                           dag=dag,
                           docker_name='''{{ params.docker_image }}''',
                           bash_command='''python {{ params.execution_dir }}/utils/scripts/backup_and_retention.py --path %s --backup_required %s --retention_days %s --log_level %s --dryrun %s''' % (path, retention_period, backup_required, 'DEBUG', 'False')
                           )



