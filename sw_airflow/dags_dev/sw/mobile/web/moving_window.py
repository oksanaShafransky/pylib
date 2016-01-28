import copy
from functools import wraps

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta

from sw.airflow.airflow_etcd import *
from sw.airflow.docker_bash_operator import DockerBashCommandBuilder
from sw.airflow.operators import DockerBashSensor

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
WINDOW_MODE = 'window'
SNAPHOT_MODE = 'snapshot'
WINDOW_MODE_TYPE = 'last-28'
SNAPSHOT_MODE_TYPE = 'monthly'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'depends_on_past': False,
    'email': ['barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2015, 12, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}


def generate_dags(mode):
    def is_window_dag():
        return mode == WINDOW_MODE

    def is_snapshot_dag():
        return mode == SNAPHOT_MODE

    mode_dag_template_params = dag_template_params.copy()
    if is_window_dag():
        mode_dag_template_params.update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})

    if is_snapshot_dag():
        mode_dag_template_params.update({'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

    dag = DAG(dag_id='Mobile_Web_' + mode.capitalize(),
              default_args=dag_args,
              params=mode_dag_template_params,
              schedule_interval=(timedelta(days=1)) if (is_window_dag()) else '0 0 l * *')

    mobile_estimation = ExternalTaskSensor(external_dag_id='Mobile_Web_Estimation', dag=dag,
                                           task_id="Mobile_Estimation",
                                           external_task_id='Estimation')
    # ! isProcSuccess MobileDailyEst daily ${date} PRODUCTION ||
    # ! isProcSuccess DesktopEstimationAggregation window ${date} PRODUCTION) ; then


    should_run_mw_window = DockerBashSensor(dag=dag,
                                            task_id="should_run_mw_window",
                                            docker_name='''{{ params.cluster }}''',
                                            bash_command='''{{ params.execution_dir }}/mobile/scripts/should_run_mw_window.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                                            )
    should_run_mw_window.set_upstream(mobile_estimation)

    builder = DockerBashCommandBuilder(
            base_data_dir='''{{ params.base_hdfs_dir }}''',
            mode='''{{ params.mode }}''',
            mode_type='''{{ params.mode_type }}''',
            dag=dag,
            date_template='''{{ ds }}''',
            docker_name='''{{ params.cluster }}''',
            script_path='''{{ params.execution_dir }}/mobile/scripts/web''') \
        .add_cmd_component('''-env main''')

    @wraps(builder.build)
    def build(task_id, core_command):
        return builder.build(task_id=task_id, core_command=core_command)

    ########################
    # Prepare HBase Tables #
    ########################

    prepare_hbase_tables = \
        build(task_id='prepare_hbase_tables', core_command='../start-process.sh -p tables -fl MOBILE_WEB') \
            .set_upstream(should_run_mw_window)

    ##############
    # Mobile Web #
    ##############
    popular_pages_agg = \
        build(task_id='popular_pages_agg', core_command='popular_pages.sh -p aggregate_popular_pages').set_upstream(
                should_run_mw_window)

    popular_pages_top_store = \
        build(task_id='popular_pages_top_store', core_command='popular_pages.sh -p top_popular_pages').set_upstream(
                [prepare_hbase_tables, popular_pages_agg])

    gaps_filler = \
        build(task_id='gaps_filler', core_command='mobile_web_gaps_filler.sh').set_upstream(should_run_mw_window)

    first_stage_agg = \
        build(task_id='first_stage_agg', core_command='first_stage_agg.sh').set_upstream(should_run_mw_window)

    # ############################################################################################################
    # the reason for new builder here is that we control whether to use new algo through mode and mode type params

    new_algo_builder = copy.copy(builder)
    new_algo_builder.mode = 'window'
    new_algo_builder.mode_type = 'last-28'
    new_algo_builder.add_cmd_component('-wenv daily-cut')

    adjust_calc_intermediate = \
        new_algo_builder.build(task_id='adjust_calc_intermediate',
                               core_command='adjust_est.sh -p prepare_data,predict').set_upstream(
                first_stage_agg)

    adjust_calc = \
        new_algo_builder.build(task_id='adjust_calc', core_command='adjust_est.sh -p redist').set_upstream(
                [gaps_filler, adjust_calc_intermediate])
    # ###########################################################################################################

    check_daily_estimations = \
        build(task_id='check_daily_estimations', core_command='check_daily_estimations.sh').set_upstream(
                adjust_calc)

    calc_subdomains = \
        build(task_id='calc_subdomains', core_command='calc_subdomains.sh').set_upstream(
                [adjust_calc, prepare_hbase_tables])

    if is_snapshot_dag():
        predict_validate_preparation = \
            build(task_id='predict_validate_preparation',
                  core_command='second_stage_tests.sh -wenv daily-cut -p prepare_total_device_count').set_upstream(
                    should_run_mw_window)

        predict_validate = \
            build(task_id='predict_validate',
                  core_command='second_stage_tests.sh -wenv daily-cut -p prepare_predictions_for_test,verify_predictions'
                  ).set_upstream([predict_validate_preparation, adjust_calc])

        compare_est_to_qc = \
            build(task_id='compare_est_to_qc', core_command='compare_estimations_to_qc.sh -sm').set_upstream(
                    first_stage_agg)

    days_to_compute_back = 31
    if is_window_dag():
        days_to_compute_back = int(WINDOW_MODE_TYPE.split('-')[1])

    sum_ww_builder = copy.copy(builder)
    sum_ww_builder.date_template = '''{{ macros.ds_add(ds,-1) }}'''
    sum_ww_builder.mode = 'daily'

    sum_ww_all = [sum_ww_builder.build(task_id='sum_ww_day_%s' % i, core_command='sum_ww.sh').set_upstream(adjust_calc)
                  for i in range(0, days_to_compute_back)]

    adjust_store_deps = [prepare_hbase_tables] + sum_ww_all
    adjust_store = build(task_id='adjust_store', core_command='adjust_est.sh -p store -ww').set_upstream(
            adjust_store_deps)

    mobile_web_pre = DummyOperator(task_id=dag.dag_id, dag=dag)
    if is_snapshot_dag():
        mobile_web_pre.set_upstream(
                [adjust_store, calc_subdomains, popular_pages_top_store,
                 predict_validate_preparation, predict_validate, compare_est_to_qc])
    else:
        mobile_web_pre.set_upstream([adjust_store, calc_subdomains, popular_pages_top_store])

    return dag


snapshot_dag = generate_dags(SNAPHOT_MODE)
window_dag = generate_dags(WINDOW_MODE)
