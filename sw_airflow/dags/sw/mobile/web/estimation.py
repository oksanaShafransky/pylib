from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from sw.airflow.docker_bash_operator import DockerBashCommandBuilder
from sw.airflow.key_value import KeyValueSetOperator

__author__ = 'Barak Gitsis'

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

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'code': DEFAULT_EXECUTION_DIR + '/mobile/scripts/web',
                       'docker_gate': DOCKER_MANAGER,
                       'data_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='Mobile_Web_Estimation', default_args=dag_args, params=dag_template_params, schedule_interval="@daily")

mobile_preliminary = ExternalTaskSensor(external_dag_id='Mobile_Preliminary', dag=dag, task_id="Mobile_Preliminary",
                                        external_task_id='Preliminary')

b = DockerBashCommandBuilder(
        docker_name='''{{ params.cluster }}''',
        script_path='''{{ params.code }}''',
        dag=dag,
        base_data_dir='''{{ params.data_dir }}''',
        date_template='''{{ ds }}''')


def register_sums_and_estimation(b, env):
    b.reset_cmd_components().add_cmd_component('-env %s' % env)
    sums = b.build(task_id='%s_sums' % env, core_command='daily_est.sh -p source_sums').set_upstream(mobile_preliminary)
    estimation = b.build(task_id='%s_estimation' % env, core_command='daily_est.sh -p est').set_upstream(sums)
    b.build(task_id='%s_estimation_check' % env,
            core_command='check_first_stage_estimates.sh').set_upstream(estimation)
    return estimation


main_estimation = register_sums_and_estimation(b, 'main')
daily_cut_estimation = register_sums_and_estimation(b, 'daily-cut')
weights = b.build(task_id='daily_cut_weights', core_command='daily_est.sh -p weights').set_upstream(
        daily_cut_estimation)
b.build(task_id='daily_cut_weights_check',
        core_command='check_weight_calculations.sh').set_upstream(weights)

# Wrap-up
register_success = KeyValueSetOperator(task_id='register_success', dag=dag,
                                       path='''services/mobile-web-daily-est/daily/{{ ds }}''',
                                       env='''{{ params.run_environment }}''')
register_success.set_upstream([main_estimation, weights])

mobile_web_estimation = DummyOperator(task_id='mobile_web_estimation', dag=dag)
mobile_web_estimation.set_upstream(register_success)
