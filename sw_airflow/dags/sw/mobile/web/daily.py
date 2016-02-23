from airflow.models import DAG
from datetime import timedelta, datetime

from sw.airflow.docker_bash_operator import DockerBashOperatorFactory
from sw.airflow.external_sensors import AdaptedExternalTaskSensor

WINDOW_MODE = 'window'
WINDOW_MODE_TYPE = 'last-28'
SNAPSHOT_MODE = 'snapshot'
SNAPSHOT_MODE_TYPE = 'monthly'

dag_args = {
    'owner': 'MobileWeb',
    'depends_on_past': False,
    'email': ['barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2016, 1, 20),
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': '/similargroup/production',
                       'docker_gate': 'docker-a02.sg.internal',
                       'base_data_dir': '/similargroup/data/mobile-analytics',
                       'cluster': 'mrp',
                       'mode': 'window',
                       'mode_type': 'last-28'
                       }

dag = DAG(dag_id='MobileWeb_Daily', schedule_interval='@daily', default_args=dag_args, params=dag_template_params)

estimation = AdaptedExternalTaskSensor(external_dag_id='MobileWeb_Estimation', dag=dag,
                                       task_id="MobileWeb_Estimation",
                                       external_task_id='Estimation')

desktop_estimation_aggregation = AdaptedExternalTaskSensor(external_dag_id='Desktop_DailyEstimation',
                                                           dag=dag,
                                                           task_id='MonthlySumEstimationParameters',
                                                           external_task_id='SumEstimation')

factory = DockerBashOperatorFactory(use_defaults=True,
                                    dag=dag,
                                    script_path='''{{ params.execution_dir }}/mobile/scripts/web''',
                                    additional_cmd_components=['-env main'])

first_stage_agg = factory.build(task_id='first_stage_agg', core_command='first_stage_agg.sh')
first_stage_agg.set_upstream(estimation)

prepare_data = factory.build(task_id='prepare_data', core_command='adjust_est.sh -p prepare_data -wenv daily-cut')
prepare_data.set_upstream([first_stage_agg, desktop_estimation_aggregation])

predict = factory.build(task_id='predict', core_command='adjust_est.sh -p predict -wenv daily-cut')
predict.set_upstream(prepare_data)

redist = factory.build(task_id='redist', core_command='adjust_est.sh -p redist -wenv daily-cut', depends_on_past=True)
redist.set_upstream(predict)

sum_ww = factory.build(task_id='sum_ww', core_command='sum_ww.sh')
sum_ww.set_upstream(redist)

check_daily_estimations = factory.build(task_id='check_daily_estimations', core_command='check_daily_estimations.sh')
check_daily_estimations.set_upstream(redist)
