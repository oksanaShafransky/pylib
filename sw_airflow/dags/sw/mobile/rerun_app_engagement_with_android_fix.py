__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator


DEFAULT_EXECUTION_DIR = '/home/iddoa/similargroup_SIM-5968_Add_android_fix_contributed_by_tal'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2015, 12, 01),
    'depends_on_past': True,
    'email': ['iddoaviram@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'adhoc' : False

}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='RecoverAppEngagement_Android51Fix', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))

# define stages

#mobile_daily_preliminary = ExternalTaskSensor(external_dag_id='MobileDailyPreliminary',
#                                    dag=dag,
#                                    task_id="EstimationPreliminary",
#                                    external_task_id='FinishProcess')

mobile_daily_preliminary = \
    DummyOperator(task_id='MobileDailyPreliminary',
                  dag=dag
                  )


app_source_weight_calculation = \
    DockerBashOperator(task_id='AppSourceWeightCalculation',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/app_engagement_daily.sh -f -d {{ ds }} -bd {{ base_hdfs_dir }} -env android_fix_source -p sqs_weight_calc'''
                       )

app_source_weight_calculation.set_upstream(mobile_daily_preliminary)

app_source_weight_smoothing_calculation = \
    DockerBashOperator(task_id='AppSourceWeightSmoothingCalculation',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/app_engagement_daily.sh -f -d {{ ds }} -bd {{ base_hdfs_dir }} -env android_fix_source -p sqs_weight_smoothing_calc'''
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
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/app_engagement_daily.sh -f -d {{ ds }} -bd {{ base_hdfs_dir }} -env android_fix_source -p prep_ratios'''
                       )

app_engagement_prior.set_upstream(mobile_daily_preliminary)

app_engagement_estimate = \
    DockerBashOperator(task_id='AppEngagementEstimate',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/app_engagement_daily.sh -f -d {{ ds }} -bd {{ base_hdfs_dir }} -env android_fix_source -p estimate'''
                       )

app_engagement_estimate.set_upstream([app_engagement_prior,app_source_quality_score])

app_engagement_daily = \
    DummyOperator(task_id='AppEngagementDaily',
                  dag=dag
                  )

app_engagement_daily.set_upstream(app_engagement_estimate)

