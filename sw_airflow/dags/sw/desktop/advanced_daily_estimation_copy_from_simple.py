__author__ = 'Kfir Eittan'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from sw.airflow.external_sensors import AdaptedExternalTaskSensor
from sw.airflow.docker_bash_operator import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
DOCKER_IMAGE = 'mrp.retention'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 3, 3),
    'depends_on_past': False,
    'email': ['andrews@similarweb.com', 'kfire@similarweb.com', 'n7i6d2a2m1h2l3f6@similar.slack.com',
              'airflow@similarweb.pagerduty.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER, 'docker_image': DOCKER_IMAGE}

dag = DAG(dag_id='Advanced_DailyEstimation_Copy_From_Simple', default_args=dag_args, params=dag_template_params,
          schedule_interval=timedelta(days=1))


estimation = AdaptedExternalTaskSensor(external_dag_id='Desktop_DailyEstimation',
                                       external_task_id='DailyEstimation',
                                       dag=dag,
                                       task_id="Estimation")

copy_estimation = \
    DockerBashOperator(task_id='CopyEstimation',
                       dag=dag,
                       docker_name='''{{ params.docker_image }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/copy_estimation_from_simple_to_advanced.sh -d {{ ds }}'''
                       )
copy_estimation.set_upstream(estimation)

###########
# Wrap-up #
###########

wrap_up = \
    DummyOperator(task_id='DailyEstimation',
                  dag=dag
                  )

wrap_up.set_upstream(copy_estimation)
