__author__ = 'Kfir Eittan'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from sw.airflow.external_sensors import AdaptedExternalTaskSensor
from sw.airflow.key_value import *
from sw.airflow.docker_bash_operator import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/adv_trk'
BASE_DIR = '/similargroup/data/advanced-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 3, 1),
    'depends_on_past': False,
    'email': ['andrews@similarweb.com', 'kfire@similarweb.com', 'n7i6d2a2m1h2l3f6@similar.slack.com',
              'airflow@similarweb.pagerduty.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='Desktop_DailyEstimation', default_args=dag_args, params=dag_template_params,
          schedule_interval=timedelta(days=1))


preliminary = AdaptedExternalTaskSensor(external_dag_id='Advanced_Preliminary',
                                        external_task_id='DesktopPreliminary',
                                        dag=dag,
                                        task_id="Preliminary")
#########################
# estimation
#########################

estimation = \
    DockerBashOperator(task_id='Estimation',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -p daily_estimation'''
                       )

estimation.set_upstream(preliminary)

add_totals_est = \
    DockerBashOperator(task_id='AddTotalsToEstimationValues',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -p add_totals_to_keys'''
                       )

add_totals_est.set_upstream(estimation)

global_reach = \
    DockerBashOperator(task_id='CalculateGlobalReach',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -p global_reach'''
                       )

global_reach.set_upstream(add_totals_est)

fractions = \
    DockerBashOperator(task_id='CalculateFractions',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -p fractions'''
                       )

fractions.set_upstream(add_totals_est)

check = \
    DockerBashOperator(task_id='Check',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/qa/checkSiteAndCountryEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -nw 7'''
                       )

check.set_upstream(add_totals_est)

est_repair = \
    DockerBashOperator(task_id='HiveRepairDailyEstimation',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -p repair'''
                       )

est_repair.set_upstream([fractions, global_reach])

values_est = \
    DummyOperator(task_id='DailyTrafficEstimation',
                  dag=dag
                  )
values_est.set_upstream(est_repair)

daily_incoming = \
    DockerBashOperator(task_id='DailyIncoming',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyIncoming.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }}'''
                       )

daily_incoming.set_upstream(add_totals_est)

incoming_repair = \
    DockerBashOperator(task_id='HiveRepairDailyIncoming',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/dailyIncoming.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -p repair'''
                       )

incoming_repair.set_upstream(daily_incoming)

sum_estimation_parameters = \
    DockerBashOperator(task_id='SumEstimation',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m window -mt last-28 -p monthly_sum_estimation_parameters'''
                       )
sum_estimation_parameters.set_upstream(values_est)

register_available = KeyValueSetOperator(task_id='MarkDataAvailability',
                                         dag=dag,
                                         path='''services/estimation/data-available/{{ ds }}''',
                                         env='''{{ params.run_environment }}'''
                                         )

register_available.set_upstream(values_est)
register_available.set_upstream(check)
register_available.set_upstream(daily_incoming)

###########
# Wrap-up #
###########

wrap_up = \
    DummyOperator(task_id='DailyEstimation',
                  dag=dag
                  )

wrap_up.set_upstream(est_repair)
wrap_up.set_upstream(incoming_repair)
wrap_up.set_upstream(register_available)
wrap_up.set_upstream(sum_estimation_parameters)
