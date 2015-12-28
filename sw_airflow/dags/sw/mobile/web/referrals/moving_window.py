__author__ = 'Amit Rom'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
WINDOW_MODE = 'window'
SNAPHOT_MODE = 'snapshot'
WINDOW_MODE_TYPE = 'last-28'
SNAPSHOT_MODE_TYPE = 'monthly'
DEFAULT_HBASE_CLUSTER = 'hbp1'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'depends_on_past': False,
    'email': ['amitr@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION',
                       'cluster': DEFAULT_CLUSTER, 'hbase_cluster': DEFAULT_HBASE_CLUSTER}


# TODO: only snapshot for now
def generate_dags(mode):
    def is_window_dag():
        return mode == WINDOW_MODE

    def is_snapshot_dag():
        return mode == SNAPHOT_MODE

    #TODO insert the real logic here
    def is_prod_env():
        return True

    dag_args_for_mode = dag_args.copy()
    if is_window_dag():
        dag_args_for_mode.update({'start_date': datetime(2015, 12, 01)})

    if is_snapshot_dag():
        dag_args_for_mode.update({'start_date': datetime(2015, 12, 30), 'end_date': datetime(2015, 12, 30)})

    dag_template_params_for_mode = dag_template_params.copy()
    if is_window_dag():
        dag_template_params_for_mode.update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})

    if is_snapshot_dag():
        dag_template_params_for_mode.update({'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

    dag = DAG(dag_id='MobileWebReferralsMovingWindow_' + mode, default_args=dag_args_for_mode, params=dag_template_params_for_mode,
          #schedule_interval=(timedelta(days=1)) if (is_window_dag()) else '0 0 l * *')
          #Following is temporary hack until we upgrade to Airflow 1.6.x or later
          schedule_interval=timedelta(days=1))

    # aggregation
    estimation_preliminary = ExternalTaskSensor(external_dag_id='MobileWebReferralsDailyAggregation',
                                                  dag=dag,
                                                  task_id="EstimationPreliminary",
                                                  external_task_id='FinishProcess')

    # daily adjustment
    daily_adjustment = ExternalTaskSensor(external_dag_id='MobileAppsMovingWindow_window',
                                                 dag=dag,
                                                 task_id="DailyAdjustment",
                                                 external_task_id='MobileWebAdjustCalc')

    #########
    # Logic #
    #########
    prepare_hbase_tables = DockerBashOperator(task_id='PrepareHBaseTables',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/start-process.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -fl mw -p tables'''
    )

    prepare_hbase_tables.set_upstream(estimation_preliminary)

    sum_user_event_rates = DockerBashOperator(task_id='SumUserEventRates',
                                         dag=dag,
                                         docker_name='''{{ params.cluster }}''',
                                         bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/estimation.sh -d {{ ds }} -p sum_user_event_rates -env main'''
    )
    sum_user_event_rates.set_upstream(estimation_preliminary)

    sum_user_event_transitions = DockerBashOperator(task_id='SumUserEventTransitions',
                                              dag=dag,
                                              docker_name='''{{ params.cluster }}''',
                                              bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/estimation.sh -d {{ ds }} -p sum_user_event_transitions -env main'''
    )
    sum_user_event_transitions.set_upstream(estimation_preliminary)

    join_event_actions = DockerBashOperator(task_id='JoinEventActions',
                                                    dag=dag,
                                                    docker_name='''{{ params.cluster }}''',
                                                    bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/estimation.sh -d {{ ds }} -p join_event_actions -env main'''
    )
    join_event_actions.set_upstream(sum_user_event_rates)
    join_event_actions.set_upstream(sum_user_event_transitions)

    calculate_joint_estimates = DockerBashOperator(task_id='CalculateJointEstimates',
                                            dag=dag,
                                            docker_name='''{{ params.cluster }}''',
                                            bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/estimation.sh -d {{ ds }} -p calculate_joint_estimates -env main'''
    )
    calculate_joint_estimates.set_upstream(join_event_actions)

    calculate_site_referrers = DockerBashOperator(task_id='CalculateSiteReferrers',
                                                   dag=dag,
                                                   docker_name='''{{ params.cluster }}''',
                                                   bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/estimation.sh -d {{ ds }} -p calculate_site_referrers -env main'''
    )
    calculate_site_referrers.set_upstream(calculate_joint_estimates)
    calculate_site_referrers.set_upstream(estimation_preliminary)
    calculate_site_referrers.set_upstream(daily_adjustment)

    calculate_site_referrers_with_totals = DockerBashOperator(task_id='CalculateSiteReferrersWithTotals',
                                                  dag=dag,
                                                  docker_name='''{{ params.cluster }}''',
                                                  bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/estimation.sh -d {{ ds }} -p calculate_site_referrers_with_totals -env main'''
    )
    calculate_site_referrers_with_totals.set_upstream(calculate_site_referrers)

    store_site_referrers_with_totals = DockerBashOperator(task_id='StoreSiteReferrersWithTotals',
                                                              dag=dag,
                                                              docker_name='''{{ params.cluster }}''',
                                                              bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/estimation.sh -d {{ ds }} -p store_site_referrers_with_totals -env main'''
    )
    store_site_referrers_with_totals.set_upstream(calculate_site_referrers_with_totals)
    store_site_referrers_with_totals.set_upstream(prepare_hbase_tables)

    wrap_up = DummyOperator(task_id='FinishProcess', dag=dag)
    wrap_up.set_upstream(store_site_referrers_with_totals)

    return dag


globals()['dag_apps_mw_referrers_moving_window_snapshot'] = generate_dags(SNAPHOT_MODE)
#globals()['dag_apps_mw_referrers_moving_window_window'] = generate_dags(WINDOW_MODE)

