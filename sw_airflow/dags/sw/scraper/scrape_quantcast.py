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

dag = DAG(dag_id='Scraping_Quantcast', default_args=dag_args, params=dag_template_params, schedule_interval="0 20 * * *")


# define stages



scrape = DockerBashOperator(task_id='ScrapeQuantcast',
                            dag=dag,
                            docker_name='''{{ params.cluster }}''',
                            bash_command='''{{ params.execution_dir }}/scraper/scripts/quantcast.sh -m snapshot -d {{ macros.ds_format(macros.first_day_of_last_month(ds), '%Y-%m-%d') }} -td {{ macros.ds_format(macros.first_day_of_last_month(ds), '%Y-%m-%d') }}'''
                            )

check = DockerBashOperator(task_id='CheckQuantcast',
                            dag=dag,
                            docker_name='''{{ params.cluster }}''',
                            bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/qa/CheckLearningSet.sh -d {{ macros.ds_format(macros.first_day_of_last_month(ds), '%Y-%m-%d') }} -ls quantcast -c 999,840,826,804,818,756,752,724,710,702,643,642,616,620,604,554,528,504,484,458,392,380,376,372,356,360,300,276,250,203,208,170,191,156,158,152,100,76,56,40,36,32'''
)

check.set_upstream(scrape)