from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor, HdfsSensor
from datetime import timedelta

from sw.airflow.airflow_etcd import *
from sw.airflow.docker_bash_operator import DockerBashOperatorFactory

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'MobileWeb',
    'start_date': datetime(2015, 12, 1),
    'depends_on_past': True,
    'email': ['amitr@similarweb.com', 'barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='MobileWeb_ReferralsAggregation', default_args=dag_args, params=dag_template_params,
          schedule_interval=timedelta(days=1))

referrals_preliminary = ExternalTaskSensor(external_dag_id='MobileWeb_ReferralsPreliminary',
                                           dag=dag,
                                           task_id="referrals_preliminary",
                                           external_task_id='ReferralsPreliminary')

# daily adjustment - this is calculated in one of MobileWeb_ processes, either Window or snapshot in adjust_calc_redist
adjust_calc_redist_ready = \
    HdfsSensor(task_id='adjust_calc_redist_ready',
               dag=dag,
               hdfs_conn_id='hdfs_%s' % DEFAULT_CLUSTER,
               filepath='''{{ params.base_hdfs_dir }}/daily/predict/mobile-web/predkey=SiteCountryKey/{{ macros.date_partition(ds) }}/_SUCCESS''',
               execution_timeout=timedelta(minutes=600))

# daily_est.sh weights
daily_cut_weights = ExternalTaskSensor(external_dag_id='Mobile_Estimation',
                                       dag=dag,
                                       task_id="daily-cut_weights",
                                       external_task_id='daily-cut_weights')

factory = DockerBashOperatorFactory(use_defaults=True,
                                    dag=dag,
                                    script_path='''{{ params.execution_dir }}/mobile/scripts/web/referrals''',
                                    additional_cmd_components=['-env main'])

build_user_transitions = factory.build(task_id='build_user_transitions',
                                       core_command='aggregation.sh -p build_user_transitions')
build_user_transitions.set_upstream(referrals_preliminary)

count_user_domain_pvs = factory.build(task_id='count_user_domain_pvs',
                                      core_command='aggregation.sh -p count_user_domain_pvs')
count_user_domain_pvs.set_upstream(referrals_preliminary)

count_user_site2_events = factory.build(task_id='count_user_site2_events',
                                        core_command='aggregation.sh -p count_user_site2_events')
count_user_site2_events.set_upstream(build_user_transitions)

calculate_user_event_rates = factory.build(task_id='calculate_user_event_rates',
                                           core_command='aggregation.sh -p calculate_user_event_rates')
calculate_user_event_rates.set_upstream([count_user_site2_events, count_user_domain_pvs])

calculate_user_event_transitions = factory.build(task_id='calculate_user_event_transitions',
                                                 core_command='aggregation.sh -p calculate_user_event_transitions ')
calculate_user_event_transitions.set_upstream([count_user_site2_events, build_user_transitions])

adjust_direct_pvs = factory.build(task_id='adjust_direct_pvs',
                                  core_command='aggregation.sh -p adjust_direct_pvs')
adjust_direct_pvs.set_upstream([build_user_transitions, adjust_calc_redist_ready])

prepare_site_estimated_pvs = factory.build(task_id='prepare_site_estimated_pvs',
                                           core_command='aggregation.sh -p prepare_site_estimated_pvs -wenv daily-cut')
prepare_site_estimated_pvs.set_upstream(daily_cut_weights)

calculate_site_pvs_shares = factory.build(task_id='calculate_site_pvs_shares',
                                          core_command='aggregation.sh -p calculate_site_pvs_shares')
calculate_site_pvs_shares.set_upstream(prepare_site_estimated_pvs)

estimate_site_pvs = factory.build(task_id='estimate_site_pvs',
                                  core_command='aggregation.sh -p estimate_site_pvs')
estimate_site_pvs.set_upstream([calculate_site_pvs_shares, adjust_calc_redist_ready])

process_complete = DummyOperator(task_id='ReferralsAggregation', dag=dag)
process_complete.set_upstream(
        [estimate_site_pvs, adjust_direct_pvs, calculate_user_event_transitions, calculate_user_event_rates])
