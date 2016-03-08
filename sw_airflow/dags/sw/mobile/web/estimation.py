from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

from sw.airflow.docker_bash_operator import DockerBashOperatorFactory
from sw.airflow.external_sensors import AdaptedExternalTaskSensor

dag_args = {
    'owner': 'MobileWeb',
    'start_date': datetime(2016, 2, 8),
    'depends_on_past': False,
    'email': ['barakg@similarweb.com', 'amitr@similarweb.com', 'airflow@similarweb.pagerduty.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': '/similargroup/production',
                       'docker_gate': 'docker-a02.sg.internal',
                       'base_data_dir': '/similargroup/data/mobile-analytics',
                       'run_environment': 'PRODUCTION',
                       'cluster': 'mrp'
                       }

dag = DAG(dag_id='MobileWeb_Estimation', default_args=dag_args, params=dag_template_params, schedule_interval="@daily")

mobile_preliminary = AdaptedExternalTaskSensor(external_dag_id='Mobile_Preliminary', dag=dag,
                                               task_id="Mobile_Preliminary",
                                               external_task_id='Preliminary')

factory = DockerBashOperatorFactory(dag=dag,
                                    base_data_dir='''{{ params.base_data_dir }}''',
                                    date_template='''{{ ds }}''',
                                    cluster='''{{ params.cluster }}''',
                                    script_path='''{{ params.execution_dir }}/mobile/scripts/web''',
                                    force=True
                                    )


# function is located here so it is visually in its place in the flow
def register_sums_and_estimation(factory, env):
    factory.additional_cmd_components = ['-env %s' % env]

    sums = factory.build(task_id='%s_sums' % env, core_command='daily_est.sh -p source_sums')
    sums.set_upstream(mobile_preliminary)

    first_stage_estimation = factory.build(task_id='%s_first_stage_estimation' % env,
                                           core_command='daily_est.sh -p est')
    first_stage_estimation.set_upstream(sums)

    first_stage_check = factory.build(task_id='%s_first_stage_check' % env,
                                      core_command='check_first_stage_estimates.sh')
    first_stage_check.set_upstream(first_stage_estimation)
    return first_stage_estimation


daily_cut_estimation = register_sums_and_estimation(factory, env='daily-cut')

weights = factory.build(task_id='daily-cut_weights', core_command='daily_est.sh -p weights', depends_on_past=True)
weights.set_upstream(daily_cut_estimation)

weights_check = factory.build(task_id='daily-cut_weights_check', core_command='check_weight_calculations.sh')
weights_check.set_upstream(weights)

main_estimation = register_sums_and_estimation(factory, env='main')

process_complete = DummyOperator(task_id='Estimation', dag=dag, sla=timedelta(hours=8))
process_complete.set_upstream([main_estimation, weights])
