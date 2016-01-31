from functools import wraps

from airflow.models import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta

from sw.airflow.docker_bash_operator import DockerBashOperatorFactory
from sw.airflow.key_value import KeyValueSetOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 1, 24),
    'depends_on_past': True,
    'email': ['barakg@similarweb.com', 'amitr@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_data_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='Mobile_Web_Estimation', default_args=dag_args, params=dag_template_params, schedule_interval="@daily")

mobile_preliminary = ExternalTaskSensor(external_dag_id='Mobile_Preliminary', dag=dag, task_id="Mobile_Preliminary",
                                        external_task_id='Preliminary')

factory = DockerBashOperatorFactory(use_defaults=True,
                                    script_path='''{{ params.execution_dir }}/mobile/scripts/web''',
                                    dag=dag,
                                    )


# function is located here so it is visually in its place in the flow
def register_sums_and_estimation(factory, env):
    factory.cmd_components = ['-env %s' % env]

    sums = \
        factory.build(task_id='%s_sums' % env,
                      core_command='daily_est.sh -p source_sums')
    sums.set_upstream(mobile_preliminary)

    estimation = \
        factory.build(task_id='%s_estimation' % env,
                      core_command='daily_est.sh -p est')
    estimation.set_upstream(sums)

    first_stage_check = \
        factory.build(task_id='%s_estimation_check' % env,
                      core_command='check_first_stage_estimates.sh')
    first_stage_check.set_upstream(estimation)
    return estimation


daily_cut_estimation = register_sums_and_estimation(factory, env='daily-cut')

weights = \
    factory.build(task_id='daily_cut_weights', core_command='daily_est.sh -p weights')
weights.set_upstream(daily_cut_estimation)

weights_check = \
    factory.build(task_id='daily_cut_weights_check', core_command='check_weight_calculations.sh')
weights_check.set_upstream(weights)

main_estimation = register_sums_and_estimation(factory, env='main')

# Wrap-up
mobile_web_estimation = KeyValueSetOperator(task_id='Mobile_Web_Estimation',
                                            dag=dag,
                                            path='''services/mobile-web-daily-est/daily/{{ ds }}''',
                                            env='''{{ params.run_environment }}''')
mobile_web_estimation.set_upstream([main_estimation, weights])
