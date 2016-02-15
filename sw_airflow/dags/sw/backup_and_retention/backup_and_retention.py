from datetime import datetime, timedelta

from airflow.models import DAG

from sw.airflow.airflow_etcd import *
from sw.airflow.docker_bash_operator import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
DOCKER_IMAGE = 'mrp-retention'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2015, 12, 28),
    'depends_on_past': True,
    'email': ['kfire@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'pool': 'Retention',
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER, 'docker_image': DOCKER_IMAGE}

dag = DAG(dag_id='RetentionProcessDAG', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages
backup_and_retention_op = \
    DockerBashOperator(task_id='RetentionProcessTask',
                       dag=dag,
                       docker_name='''{{ params.docker_image }}''',
                       bash_command='''python {{ params.execution_dir }}/utils/scripts/backup_and_retention.py --hadoop_cluster_namenode active.hdfs-namenode-{{ params.cluster }}.service.production --s3_key_prefix /mrp --log_level %s --dry_run %s''' % ('DEBUG', 'False')
                       )



