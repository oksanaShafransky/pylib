__author__ = 'Ayush Kumar'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.docker_bash_operator import DockerBashOperator
from sw.airflow.external_sensors import AdaptedExternalTaskSensor

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics/demographics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 3, 7),
    'depends_on_past': False,
    'email': ['bigdata@similarweb.com', 'n7i6d2a2m1h2l3f6@similar.slack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='AndroidApps_Demographics', default_args=dag_args, params=dag_template_params, schedule_interval="@daily")

# define stages
preliminary = AdaptedExternalTaskSensor(external_dag_id='Mobile_Preliminary',
                                              dag=dag,
                                              task_id='Mobile_Preliminary',
                                              external_task_id='Preliminary')


group_apps = DockerBashOperator(task_id='GroupAppsByAdvId',
                            dag=dag,
                            docker_name='''{{ params.cluster }}''',
                            bash_command='''invoke -c {{ params.execution_dir }}/mobile/scripts/demographics/android/adv_id_user_grouping adv_id_user_grouping -d {{ ds }} -b {{ params.base_hdfs_dir}}'''
                            )
group_apps.set_upstream(preliminary)

done = DummyOperator(task_id='AndroidApps_Demographics', dag=dag, sla=timedelta(hours=6))
done.set_upstream(group_apps)
