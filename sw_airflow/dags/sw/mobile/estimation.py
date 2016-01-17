__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor

from sw.airflow.key_value import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 1, 14),
    'depends_on_past': True,
    'email': ['felixv@similarweb.com','iddoav@similarweb.com', 'barakg@similarweb.com','amitr@similarweb.com>'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='Mobile_Estimation', default_args=dag_args, params=dag_template_params,
          schedule_interval="@daily")


mobile_daily_preliminary = ExternalTaskSensor(external_dag_id='Mobile_Preliminary',
                                              dag=dag,
                                              task_id="Preliminary",
                                              external_task_id='Preliminary')
#########################
# Apps engagement score #
#########################

app_source_weight_calculation = \
    DockerBashOperator(task_id='AppSourceWeightCalculation',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/app_engagement_daily.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env all_countries -p sqs_weight_calc'''
                       )

app_source_weight_calculation.set_upstream(mobile_daily_preliminary)

app_source_weight_smoothing_calculation = \
    DockerBashOperator(task_id='AppSourceWeightSmoothingCalculation',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/app_engagement_daily.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env all_countries -p sqs_weight_smoothing_calc'''
                       )

app_source_weight_smoothing_calculation.set_upstream(app_source_weight_calculation)

app_source_quality_score = \
    DummyOperator(task_id='AppSourceQualityScore',
                  dag=dag
                  )

app_source_quality_score.set_upstream(app_source_weight_smoothing_calculation)

app_engagement_prior = \
    DockerBashOperator(task_id='AppEngagementPrior',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/app_engagement_daily.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env all_countries -p prep_ratios'''
                       )

app_engagement_prior.set_upstream(mobile_daily_preliminary)

app_engagement_estimate = \
    DockerBashOperator(task_id='AppEngagementEstimate',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/app_engagement_daily.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env all_countries -p estimate'''
                       )

app_engagement_estimate.set_upstream([app_engagement_prior, app_source_quality_score])

app_engagement_daily_check = \
    DockerBashOperator(task_id='AppEngagementDailyCheck',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/qa/checkAppAndCountryEngagementEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -p check_daily'''
                       )

app_engagement_daily_check.set_upstream(app_engagement_estimate)

app_engagement_daily = \
    DummyOperator(task_id='AppEngagementDaily',
                  dag=dag
                  )

app_engagement_daily.set_upstream(app_engagement_estimate)

###################
# Mobile Web Main #
###################

mobile_web_main_sums = \
    DockerBashOperator(task_id='MobileWebMainSums',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/web/daily_est.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -p source_sums'''
                       )

mobile_web_main_sums.set_upstream(mobile_daily_preliminary)

mobile_web_main_estimation = \
    DockerBashOperator(task_id='MobileWebMainEstimation',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/web/daily_est.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -p est'''
                       )

mobile_web_main_estimation.set_upstream(mobile_web_main_sums)

mobile_web_main_estimation_check = \
    DockerBashOperator(task_id='MobileWebMainEstimationCheck',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/web/check_first_stage_estimates.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main'''
                       )

mobile_web_main_estimation_check.set_upstream(mobile_web_main_estimation)

mobile_web_main = \
    DummyOperator(task_id='MobileWebMain',
                  dag=dag
                  )

mobile_web_main.set_upstream(mobile_web_main_estimation)

########################
# Mobile Web Daily Cut #
########################

mobile_web_daily_cut_sums = \
    DockerBashOperator(task_id='MobileWebDailyCutSums',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/web/daily_est.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env daily-cut -p source_sums'''
                       )

mobile_web_daily_cut_sums.set_upstream(mobile_daily_preliminary)

mobile_web_daily_cut_estimation = \
    DockerBashOperator(task_id='MobileWebDailyCutEstimation',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/web/daily_est.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env daily-cut -p est'''
                       )

mobile_web_daily_cut_estimation.set_upstream(mobile_web_daily_cut_sums)

mobile_web_daily_cut_estimation_check = \
    DockerBashOperator(task_id='MobileWebDailyCutEstimationCheck',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/web/check_first_stage_estimates.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env daily-cut'''
                       )

mobile_web_daily_cut_estimation_check.set_upstream(mobile_web_daily_cut_estimation)

mobile_web_daily_cut_weights = \
    DockerBashOperator(task_id='MobileWebDailyCutWeights',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/web/daily_est.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env daily-cut -p weights'''
                       )

mobile_web_daily_cut_weights.set_upstream(mobile_web_daily_cut_estimation)

mobile_web_daily_cut_weights_check = \
    DockerBashOperator(task_id='MobileWebDailyCutWeightsCheck',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/web/check_weight_calculations.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env daily-cut'''
                       )

mobile_web_daily_cut_weights_check.set_upstream(mobile_web_daily_cut_weights)

mobile_web_daily_cut = \
    DummyOperator(task_id='MobileWebDailyCut',
                  dag=dag
                  )

mobile_web_daily_cut.set_upstream(mobile_web_daily_cut_weights)

##############################
# Mobile Daily Usage Pattern #
##############################

mobile_daily_usage_pattern = \
    DockerBashOperator(task_id='MobileDailyUsagePattern',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/daily_est.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }}'''
                       )

mobile_daily_usage_pattern.set_upstream(mobile_daily_preliminary)

###########
# Wrap-up #
###########

mobile_apps_estimation = \
    DummyOperator(task_id='MobileAppsEstimation',
                  dag=dag
                  )
mobile_apps_estimation.set_upstream([app_engagement_daily, mobile_daily_usage_pattern])

mobile_web_estimation = \
    DummyOperator(task_id='MobileWebEstimation',
                  dag=dag
                  )
mobile_web_estimation.set_upstream([mobile_web_daily_cut, mobile_web_main])

register_success = KeyValueSetOperator(task_id='RegisterSuccessOnETCD',
                                       dag=dag,
                                       path='''services/mobile-daily-est/daily/{{ ds }}''',
                                       env='PRODUCTION'
                                       )
register_success.set_upstream([mobile_apps_estimation, mobile_web_estimation])

estimation = \
    DummyOperator(task_id='Estimation',
                  dag=dag
                  )
estimation.set_upstream(register_success)
