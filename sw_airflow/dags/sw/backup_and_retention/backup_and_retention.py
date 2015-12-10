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
    'start_date': datetime(2015, 12, 7),
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
backup_and_retention_op = \
    DockerBashOperator(task_id='HdfsBackupAndRetentionTask',
                       dag=dag,
                       docker_name='''{{ params.docker_image }}''',
                       bash_command='''python {{ params.execution_dir }}/utils/scripts/backup_and_retention.py --hadoop_cluster_namenode active.hdfs-namenode-{{ params.cluster }}.service.production --log_level %s --dry_run %s''' % ('DEBUG', 'False')
                       )



