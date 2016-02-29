__author__ = 'lajonat'

from datetime import datetime, timedelta

from airflow.models import DAG

from sw.airflow.docker_bash_operator import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 2, 28),
    'depends_on_past': False,
    'email': ['spiders@similarweb.com', 'n7i6d2a2m1h2l3f6@similar.slack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='Scraping_BackupScrapeData', default_args=dag_args, params=dag_template_params, schedule_interval="0 1 * * *")


# define stages


backup = DockerBashOperator(task_id='BackupData',
                            dag=dag,
                            docker_name='''{{ params.cluster }}''',
                            bash_command='''{{ params.execution_dir }}/scraper/scripts/backupScraperData.sh {{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y %m %d') }} -f'''
                            )