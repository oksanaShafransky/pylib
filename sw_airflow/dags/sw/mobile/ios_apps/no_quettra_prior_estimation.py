__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator

from airflow.models import DAG
from sw.airflow.external_sensors import AdaptedExternalTaskSensor

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/home/iddoa/similargroup_SIM-7965/study-alternative-quettra-ios-apps-estimation'
BASE_DIR = '/user/iddoa/ios-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp-ios'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2015, 11, 1),
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com', 'n7i6d2a2m1h2l3f6@similar.slack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER,
                       'mode': 'window', 'mode_type': 'last-28'}

dag = DAG(dag_id='IosApps_NoQuettraPriorEstimation', default_args=dag_args, params=dag_template_params,
          schedule_interval='@daily')

preliminary = AdaptedExternalTaskSensor(external_dag_id='IosApps_Preliminary',
                                              dag=dag,
                                              task_id='Preliminary',
                                              external_task_id='Preliminary')

reach_estimate = DockerBashOperator(task_id='ReachEstimate',
                                    dag=dag,
                                    docker_name=DEFAULT_CLUSTER,
                                    bash_command='''invoke  -c {{ params.execution_dir }}/mobile/scripts/app-engagement/ios/reach_estimate reach_estimate -d {{ ds }} -b {{ params.base_hdfs_dir }}'''
                                    )

reach_estimate.set_upstream(preliminary)

wrap_up = DummyOperator(task_id='Estimation',
                        dag=dag
                        )

wrap_up.set_upstream(reach_estimate)


