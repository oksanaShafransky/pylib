import copy

__author__ = 'Barak Gitsis'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor

from sw.airflow.key_value import *

from sw.airflow.docker_bash_operator import DockerBashOperatorBuilder

DEFAULT_EXECUTION_DIR = '/similargroup/production'
SCRIPTS_DIR = DEFAULT_EXECUTION_DIR + '/mobile/scripts/web'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 1, 24),
    'depends_on_past': True,
    'email': ['barakg@similarweb.com', 'amitr@similarweb.com>'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'code': SCRIPTS_DIR,
                       'docker_gate': DOCKER_MANAGER,
                       'data_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='Mobile_Web_Estimation', default_args=dag_args, params=dag_template_params, schedule_interval="@daily")
dag.p = dag_template_params

mobile_preliminary = ExternalTaskSensor(external_dag_id='Mobile_Preliminary',
                                        dag=dag,
                                        task_id="mobile_preliminary",
                                        external_task_id='Preliminary')

###################
# Mobile Web Main #
###################
builder = DockerBashOperatorBuilder() \
    .set_docker_name('''{{ params.cluster }}''') \
    .set_script_path('''{{ params.code }}''') \
    .set_dag(dag) \
    .set_base_data_dir('''{{ params.data_dir }}''') \
    .set_date_template('''{{ ds }}''') \
    .add_cmd_component('-env main')

main_sums = builder.build(task_id='main_sums', core_command='daily_est.sh -p source_sums')
main_sums.set_upstream(mobile_preliminary)

main_estimation = builder.build(task_id='main_estimation', core_command='daily_est.sh -p est')
main_estimation.set_upstream(main_sums)

main = DummyOperator(task_id='main', dag=dag)
main.set_upstream(main_estimation)

main_estimation_check = builder.build(task_id='main_estimation_check',
                                      core_command='check_first_stage_estimates.sh')
main_estimation_check.set_upstream(main_estimation)

########################
# Mobile Web Daily Cut #
########################
dc_builder = builder.clone().reset_cmd_components().add_cmd_component('-env daily-cut')

daily_cut_sums = dc_builder.build(task_id='daily_cut_sums', core_command='daily_est.sh -p source_sums')
daily_cut_sums.set_upstream(mobile_preliminary)

daily_cut_estimation = dc_builder.build(task_id='daily_cut_estimation', core_command='daily_est.sh -p est')
daily_cut_estimation.set_upstream(daily_cut_sums)

daily_cut_estimation_check = dc_builder.build(task_id='daily_cut_estimation_check',
                                              core_command='check_first_stage_estimates.sh')
daily_cut_estimation_check.set_upstream(daily_cut_estimation)

daily_cut_weights = dc_builder.build(task_id='daily_cut_weights', core_command='daily_est.sh -p weights')
daily_cut_weights.set_upstream(daily_cut_estimation)

daily_cut = DummyOperator(task_id='daily_cut', dag=dag)
daily_cut.set_upstream(daily_cut_weights)

daily_cut_weights_check = dc_builder.build(task_id='daily_cut_weights_check',
                                           core_command='check_weight_calculations.sh')
daily_cut_weights_check.set_upstream(daily_cut_weights)

###########
# Wrap-up #
###########
register_success = KeyValueSetOperator(task_id='register_success', dag=dag,
                                       path='''services/mobile-web-daily-est/daily/{{ ds }}''',
                                       env='DEV'
                                       )
register_success.set_upstream([daily_cut, main])

mobile_web_estimation = DummyOperator(task_id='mobile_web_estimation', dag=dag)
mobile_web_estimation.set_upstream(register_success)
