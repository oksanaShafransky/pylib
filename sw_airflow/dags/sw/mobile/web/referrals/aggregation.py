__author__ = 'Amit Rom'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2015, 12, 1),
    'depends_on_past': False,
    'email': ['amitr@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='MobileWebReferralsDailyAggregation', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# TODO: sensor for preliminary done
# optimize parallel
preliminary_data_ready = EtcdSensor(task_id='OperaRawDataReady',
                                    dag=dag,
                                    root=ETCD_ENV_ROOT[dag_template_params['run_environment']],
                                    path='''services/opera-mini-s3/daily/{{ ds }}'''
)


build_user_transitions = DockerBashOperator(task_id='BuildUserTransitions',
                                     dag=dag,
                                     docker_name='''{{ params.cluster }}''',
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/aggregation.sh -d {{ ds }} -p build_user_transitions -env main'''
)
build_user_transitions.set_upstream(preliminary_data_ready)

count_user_domain_pvs = DockerBashOperator(task_id='CountUserDomainPVs',
                                            dag=dag,
                                            docker_name='''{{ params.cluster }}''',
                                            bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/aggregation.sh -d {{ ds }} -p count_user_domain_pvs -env main'''
)
count_user_domain_pvs.set_upstream(preliminary_data_ready)

count_user_site2_events = DockerBashOperator(task_id='CountUserSiteSite2Events',
                                           dag=dag,
                                           docker_name='''{{ params.cluster }}''',
                                           bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/aggregation.sh -d {{ ds }} -p count_user_site2_events -env main'''
)
count_user_domain_pvs.set_upstream(build_user_transitions)

calculate_user_event_rates = DockerBashOperator(task_id='CalculateUserEventRates',
                                             dag=dag,
                                             docker_name='''{{ params.cluster }}''',
                                             bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/aggregation.sh -d {{ ds }} -p calculate_user_event_rates -env main'''
)
calculate_user_event_rates.set_upstream(count_user_site2_events)
calculate_user_event_rates.set_upstream(count_user_domain_pvs)

calculate_user_event_transitions = DockerBashOperator(task_id='CalculateUserEventTransitions',
                                                dag=dag,
                                                docker_name='''{{ params.cluster }}''',
                                                bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/aggregation.sh -d {{ ds }} -p calculate_user_event_transitions -env main'''
)
calculate_user_event_transitions.set_upstream(count_user_site2_events)
calculate_user_event_transitions.set_upstream(build_user_transitions)

adjust_direct_pvs = DockerBashOperator(task_id='AdjustDirectPVs',
                                                      dag=dag,
                                                      docker_name='''{{ params.cluster }}''',
                                                      bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/aggregation.sh -d {{ ds }} -p adjust_direct_pvs -env main'''
)
adjust_direct_pvs.set_upstream(build_user_transitions)
# TODO: daily adjustment should be dependency

prepare_site_estimated_pvs = DockerBashOperator(task_id='PrepareSiteEstimatedPVs',
                                       dag=dag,
                                       docker_name='''{{ params.cluster }}''',
                                       bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/aggregation.sh -d {{ ds }} -p prepare_site_estimated_pvs -env main -wenv daily-cut'''
)

# TODO: daily weights should be dependency

calculate_site_pvs_shares = DockerBashOperator(task_id='CalculateSitePVsShares',
                                                dag=dag,
                                                docker_name='''{{ params.cluster }}''',
                                                bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/aggregation.sh -d {{ ds }} -p calculate_site_pvs_shares -env main'''
)


calculate_site_pvs_shares.set_upstream(prepare_site_estimated_pvs)

estimate_site_pvs = DockerBashOperator(task_id='EstimateSitePVs',
                                               dag=dag,
                                               docker_name='''{{ params.cluster }}''',
                                               bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/aggregation.sh -d {{ ds }} -p estimate_site_pvs -env main'''
)


estimate_site_pvs.set_upstream(calculate_site_pvs_shares)
# TODO: daily adjustment should be dependency