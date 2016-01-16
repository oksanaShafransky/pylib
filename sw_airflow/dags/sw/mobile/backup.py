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
    'start_date': datetime(16, 1, 15),
    'depends_on_past': False,
    'email': ['bigdata@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='Mobile_Backup', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages

group_files_base = '/similargroup/data/stats-mobile/raw'
should_run = HdfsSensor(task_id='GroupFilesReady',
                        dag=dag,
                        hdfs_conn_id='hdfs_%s' % DEFAULT_CLUSTER,
                        filepath='''%s/{{ macros.date_partition(ds) }}/_SUCCESS''' % group_files_base,
                        execution_timeout=timedelta(minutes=240)
                        )


upload = DockerBashOperator(task_id='UploadToS3',
                            dag=dag,
                            docker_name='''{{ params.cluster }}''',
                            bash_command='''{{ params.execution_dir }}/mobile/scripts/preliminary/group_raw.sh -d {{ ds }} -p backup'''
                            )
upload.set_upstream(should_run)



