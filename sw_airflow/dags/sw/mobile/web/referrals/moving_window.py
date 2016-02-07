from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta

from sw.airflow.airflow_etcd import *
from sw.airflow.docker_bash_operator import DockerBashOperatorFactory

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'MobileWeb',
    'depends_on_past': False,
    'start_date': datetime(2016, 2, 10),
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
                       'docker_image_name': 'mrp',
                       'mode': 'snapshot',
                       'mode_type': 'monthly'}

snapshot_dag = DAG(dag_id='MobileWeb_ReferralsSnapshot', default_args=dag_args, params=dag_template_params,
                   schedule_interval='@monthly')


def assemble_process(dag):
    calculate_user_event_rates = ExternalTaskSensor(external_dag_id='MobileWeb_ReferralsDaily',
                                                    dag=dag, task_id="calculate_user_event_rates",
                                                    external_task_id='calculate_user_event_rates')

    calculate_user_event_transitions = ExternalTaskSensor(external_dag_id='MobileWeb_ReferralsDaily',
                                                          dag=dag, task_id="calculate_user_event_transitions",
                                                          external_task_id='calculate_user_event_transitions')

    estimate_site_pvs = ExternalTaskSensor(external_dag_id='MobileWeb_ReferralsDaily',
                                           dag=dag, task_id="estimate_site_pvs",
                                           external_task_id='estimate_site_pvs')

    adjust_direct_pvs = ExternalTaskSensor(external_dag_id='MobileWeb_ReferralsDaily',
                                           dag=dag, task_id="adjust_direct_pvs",
                                           external_task_id='adjust_direct_pvs')

    adjust_calc_redist = ExternalTaskSensor(external_dag_id='MobileWeb_Snapshot',
                                            dag=dag, task_id="adjust_calc_redist",
                                            external_task_id='adjust_calc_redist')

    #########
    # Logic #
    #########
    factory = DockerBashOperatorFactory(use_defaults=True,
                                        dag=dag,
                                        script_path='''{{ params.execution_dir }}/mobile/scripts/web/referrals''',
                                        additional_cmd_components=['-env main'])

    sum_user_event_rates = factory.build(task_id='sum_user_event_rates',
                                         core_command='estimation.sh -p sum_user_event_rates')
    sum_user_event_rates.set_upstream(calculate_user_event_rates)

    sum_user_event_transitions = factory.build(task_id='sum_user_event_transitions',
                                               core_command='estimation.sh -p sum_user_event_transitions')
    sum_user_event_transitions.set_upstream([calculate_user_event_transitions, estimate_site_pvs])

    join_event_actions = factory.build(task_id='join_event_actions',
                                       core_command='estimation.sh -p join_event_actions')
    join_event_actions.set_upstream([sum_user_event_rates, sum_user_event_transitions])

    calculate_joint_estimates = factory.build(task_id='calculate_joint_estimates',
                                              core_command='estimation.sh -p calculate_joint_estimates')
    calculate_joint_estimates.set_upstream(join_event_actions)

    calculate_site_referrers = factory.build(task_id='calculate_site_referrers',
                                             core_command='estimation.sh -p calculate_site_referrers')
    calculate_site_referrers.set_upstream([calculate_joint_estimates, adjust_direct_pvs, adjust_calc_redist])

    calculate_site_referrers_with_totals = \
        factory.build(task_id='calculate_site_referrers_with_totals',
                      core_command='estimation.sh -p calculate_site_referrers_with_totals')
    calculate_site_referrers_with_totals.set_upstream(calculate_site_referrers)

    ##################
    # load to hbase #
    ##################
    prepare_hbase_tables = ExternalTaskSensor(external_dag_id='MobileWeb_Window',
                                              dag=dag, task_id="prepare_hbase_tables",
                                              external_task_id='prepare_hbase_tables')

    load_site_referrers_with_totals = factory.build(task_id='load_site_referrers_with_totals',
                                                    core_command='mwr_load.sh -p load_site_referrers_with_totals')
    load_site_referrers_with_totals.set_upstream([calculate_site_referrers_with_totals, prepare_hbase_tables])
    calculate_traffic_dist_month = factory.build(task_id='calculate_traffic_dist_month',
                                                 core_command='mwr_load.sh -p calculate_traffic_dist_month')
    calculate_traffic_dist_month.set_upstream([calculate_site_referrers_with_totals, prepare_hbase_tables])

    mobile_web_referrals_mw = DummyOperator(task_id=dag.dag_id, dag=dag)
    mobile_web_referrals_mw.set_upstream([load_site_referrers_with_totals, calculate_traffic_dist_month])


assemble_process(snapshot_dag)
