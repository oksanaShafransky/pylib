from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor, HdfsSensor
from datetime import timedelta, datetime

from sw.airflow.docker_bash_operator import DockerBashOperatorFactory
from sw.airflow.key_value import KeyValueSensor

dag_args = {
    'owner': 'MobileWeb',
    'start_date': datetime(2016, 2, 1),
    'depends_on_past': True,
    'email': ['amitr@similarweb.com', 'barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': '/similargroup/production',
                       'docker_gate': 'docker-a02.sg.internal',
                       'base_data_dir': '/similargroup/data/mobile-analytics',
                       'run_environment': 'PRODUCTION',
                       'cluster': 'mrp'
                       }

dag = DAG(dag_id='MobileWeb_ReferralsDaily', default_args=dag_args, params=dag_template_params,
          schedule_interval='@daily')

factory = DockerBashOperatorFactory(use_defaults=True,
                                    dag=dag,
                                    script_path='''{{ params.execution_dir }}/mobile/scripts/web/referrals''',
                                    additional_cmd_components=['-env main'])

opera_raw_data_ready = KeyValueSensor(task_id='opera_raw_data_ready',
                                      dag=dag,
                                      env='''{{ params.run_environment }}''',
                                      path='''services/opera-mini-s3/daily/{{ ds }}''',
                                      execution_timeout=timedelta(minutes=1)
                                      )

filter_malformed_events = factory.build(task_id='filter_malformed_events',
                                        core_command='preliminary.sh -p filter_malformed_events')
filter_malformed_events.set_upstream(opera_raw_data_ready)

extract_invalid_users = factory.build(task_id='extract_invalid_users',
                                      core_command='preliminary.sh -p filter_users ')
extract_invalid_users.set_upstream(filter_malformed_events)

filter_invalid_users = factory.build(task_id='filter_invalid_users',
                                     core_command='preliminary.sh -p filter_invalid_users_from_events''')
filter_invalid_users.set_upstream(extract_invalid_users)

build_user_transitions = factory.build(task_id='build_user_transitions',
                                       core_command='aggregation.sh -p build_user_transitions')
build_user_transitions.set_upstream(filter_invalid_users)

count_user_domain_pvs = factory.build(task_id='count_user_domain_pvs',
                                      core_command='aggregation.sh -p count_user_domain_pvs')
count_user_domain_pvs.set_upstream(filter_invalid_users)

count_user_site2_events = factory.build(task_id='count_user_site2_events',
                                        core_command='aggregation.sh -p count_user_site2_events')
count_user_site2_events.set_upstream(build_user_transitions)

calculate_user_event_rates = factory.build(task_id='calculate_user_event_rates',
                                           core_command='aggregation.sh -p calculate_user_event_rates')
calculate_user_event_rates.set_upstream([count_user_site2_events, count_user_domain_pvs])

calculate_user_event_transitions = factory.build(task_id='calculate_user_event_transitions',
                                                 core_command='aggregation.sh -p calculate_user_event_transitions ')
calculate_user_event_transitions.set_upstream([count_user_site2_events, build_user_transitions])

# daily adjustment - this is calculated in one of MobileWeb_ processes, either Window or snapshot in adjust_calc_redist
adjust_calc_redist_ready = \
    HdfsSensor(task_id='adjust_calc_redist_ready',
               dag=dag,
               hdfs_conn_id='''hdfs_{{ params.cluster }}''',
               filepath='''{{ params.base_hdfs_dir }}/daily/predict/mobile-web/predkey=SiteCountryKey/{{ macros.date_partition(ds) }}/_SUCCESS''',
               execution_timeout=timedelta(minutes=600))

adjust_direct_pvs = factory.build(task_id='adjust_direct_pvs',
                                  core_command='aggregation.sh -p adjust_direct_pvs')
adjust_direct_pvs.set_upstream([build_user_transitions, adjust_calc_redist_ready])

# daily_est.sh weights
daily_cut_weights = ExternalTaskSensor(external_dag_id='Mobile_Estimation',
                                       dag=dag,
                                       task_id="daily-cut_weights",
                                       external_task_id='daily-cut_weights')

prepare_site_estimated_pvs = factory.build(task_id='prepare_site_estimated_pvs',
                                           core_command='aggregation.sh -p prepare_site_estimated_pvs -wenv daily-cut')
prepare_site_estimated_pvs.set_upstream(daily_cut_weights)

calculate_site_pvs_shares = factory.build(task_id='calculate_site_pvs_shares',
                                          core_command='aggregation.sh -p calculate_site_pvs_shares')
calculate_site_pvs_shares.set_upstream(prepare_site_estimated_pvs)

estimate_site_pvs = factory.build(task_id='estimate_site_pvs',
                                  core_command='aggregation.sh -p estimate_site_pvs')
estimate_site_pvs.set_upstream([calculate_site_pvs_shares, adjust_calc_redist_ready])

process_complete = DummyOperator(task_id='ReferralsDaily', dag=dag)
process_complete.set_upstream(
        [estimate_site_pvs, adjust_direct_pvs, calculate_user_event_transitions, calculate_user_event_rates])
