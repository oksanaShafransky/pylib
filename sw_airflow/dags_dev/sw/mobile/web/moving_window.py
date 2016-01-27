__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from sw.airflow.airflow_etcd import *
from sw.airflow.docker_bash_operator import DockerBashOperator, DockerBashOperatorBuilder
from sw.airflow.operators import DockerBashSensor
from sw.airflow.airflow_etcd import EtcdHook
from airflow.operators.python_operator import BranchPythonOperator

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

    def is_prod_env():
        return True

    if is_window_dag():
        dag_args.update({'start_date': datetime(2015, 12, 23)})

    if is_snapshot_dag():
        dag_args.update({'start_date': datetime(2015, 12, 31)})

    dag_template_params_for_mode = dag_template_params.copy()
    mode_dag_template_params = {}

    if is_window_dag():
        mode_dag_template_params = {'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE}

    if is_snapshot_dag():
        mode_dag_template_params = {'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE}

    dag_template_params_for_mode.update(mode_dag_template_params)

    dag = DAG(dag_id='Mobile_Web_Moving_Window_' + mode, default_args=dag_args, params=dag_template_params_for_mode,
              schedule_interval=(timedelta(days=1)) if (is_window_dag()) else '0 0 l * *')

    mobile_daily_estimation = ExternalTaskSensor(external_dag_id='Mobile_Estimation',
                                                 dag=dag,
                                                 task_id="MobileDailyEstimation",
                                                 external_task_id='Estimation')

    b = DockerBashOperatorBuilder() \
        .set_base_data_dir('''{{ params.base_hdfs_dir }}''') \
        .add_cmd_component(''' -m {{ params.mode }} -mt {{ params.mode_type }}''') \
        .set_dag(dag) \
        .set_date_template('''{{ ds }}''') \
        .set_docker_name('''{{ params.cluster }}''') \
        .set_script_path('''{{ params.execution_dir }}/mobile/scripts''')

    should_run_mw_window = DockerBashSensor(dag=dag,
                                            task_id="ShouldRunMwWindow",
                                            docker_name='''{{ params.cluster }}''',
                                            bash_command='''{{ params.execution_dir }}/mobile/scripts/should_run_mw_window.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                                            )
    should_run_mw_window.set_upstream(mobile_daily_estimation)

    ########################
    # Prepare HBase Tables #
    ########################

    prepare_hbase_tables = \
        DockerBashOperator(task_id='prepare_hbase_tables',
                           bash_command='''start-process.sh -d {{ ds }} -p tables -fl MOBILE_WEB '''
                           )

    prepare_hbase_tables.set_upstream(should_run_mw_window)

    ##############
    # Mobile Web #
    ##############

    mobile_web_train_model = None
    if is_snapshot_dag():
        # TODO configure parallelism setting for this task, which is heavier (20 slots)
        mobile_web_predict_validate_preparation = \
            DockerBashOperator(task_id='MobileWebPredictValidatePreparation',
                               bash_command='''web/second_stage_tests.sh -d {{ ds }} -env main -wenv daily-cut -p prepare_total_device_count'''
                               )
        mobile_web_predict_validate_preparation.set_upstream(should_run_mw_window)

        # TODO configure parallelism setting for this task, which is heavier (20 slots)
        mobile_web_predict_validate = \
            DockerBashOperator(task_id='MobileWebPredictValidate',
                               bash_command='''web/second_stage_tests.sh -d {{ ds }} -env main -wenv daily-cut -p prepare_predictions_for_test,verify_predictions'''
                               )

        # TODO add dependency on adjust_calc
        mobile_web_predict_validate.set_upstream([mobile_web_predict_validate_preparation])

        # TODO configure parallelism setting for this task, which is heavier (20 slots)
        mobile_web_compare_est_to_qc = \
            DockerBashOperator(task_id='MobileWebCompareEstToQC',
                               bash_command='''web/compare_estimations_to_qc.sh -d {{ ds }} -sm -env main'''
                               )

        # TODO configure parallelism setting for this task, which is heavier (20 slots)
        mobile_web_train_model = \
            DockerBashOperator(task_id='MobileWebTrainModel',
                               bash_command='''web/train_mobile_web_model.sh -d {{ ds }} -fd {{ ds }} -env main'''
                               )

        # TODO configure parallelism setting for this task, which is heavier (20 slots)
        mobile_web_model_validate = \
            DockerBashOperator(task_id='MobileWebModelValidate',
                               bash_command='''web/second_stage_tests.sh -d {{ ds }} -fd {{ ds }} -env main -wenv daily-cut -p check_model'''
                               )
        mobile_web_model_validate.set_upstream(mobile_web_train_model)

    mobile_web_gaps_filler = \
        DockerBashOperator(task_id='MobileWebGapsFiller',
                           bash_command='''web/mobile_web_gaps_filler.sh -d {{ ds }} -env main'''
                           )
    mobile_web_gaps_filler.set_upstream(should_run_mw_window)

    # TODO I should verify: is the task ID right? should we concatenate the date?
    # TODO configure parallelism setting for this task, which is heavier (10 slots)
    mobile_web_first_stage_agg = \
        DockerBashOperator(task_id='MobileWebFirstStageAgg',
                           bash_command='''web/first_stage_agg.sh -d {{ ds }} -env main -m window -mt last-28'''
                           )
    if is_snapshot_dag():
        mobile_web_train_model.set_upstream(mobile_web_first_stage_agg)
        mobile_web_compare_est_to_qc.set_upstream(mobile_web_first_stage_agg)

    # TODO configure parallelism setting for this task, which is heavier (10 slots)
    mobile_web_first_stage_agg.set_upstream(should_run_mw_window)

    # TODO I should verify: is the task ID right? should we concatenate the date?
    # TODO configure parallelism setting for this task, which is heavier (10 slots)
    mobile_web_adjust_calc_intermediate = \
        DockerBashOperator(task_id='MobileWebAdjustCalcIntermediate',
                           bash_command='''web/adjust_est.sh -d {{ ds }} -env main -wenv daily-cut -m window -mt last-28 -p prepare_data,predict'''
                           )
    if is_snapshot_dag():
        mobile_web_adjust_calc_intermediate.set_upstream([mobile_web_train_model, mobile_web_first_stage_agg])
    else:
        mobile_web_adjust_calc_intermediate.set_upstream(mobile_web_first_stage_agg)

    # TODO configure parallelism setting for this task, which is heavier (20 slots)

    adjust_calc = \
        DockerBashOperator(task_id='adjust_calc',
                           bash_command='''web/adjust_est.sh -d {{ ds }} -env main -wenv daily-cut -m window -mt last-28 -p redist'''
                           )
    adjust_calc.set_upstream([mobile_web_gaps_filler, mobile_web_adjust_calc_intermediate])

    # TODO configure parallelism setting for this task, which is heavier (20 slots)
    mobile_web_check_daily_estimations = \
        DockerBashOperator(task_id='MobileWebCheckDailyEstimations',
                           bash_command='''web/check_daily_estimations.sh -d {{ ds }} -env main -p redist'''
                           )
    mobile_web_check_daily_estimations.set_upstream(adjust_calc)

    # TODO configure parallelism setting for this task, which is heavier (20 slots)
    mobile_web_calc_subdomains = \
        DockerBashOperator(task_id='MobileWebCalcSubdomains',
                           bash_command='''web/calc_subdomains.sh -d {{ ds }} -env main'''
                           )
    mobile_web_calc_subdomains.set_upstream([adjust_calc, prepare_hbase_tables])

    # TODO configure parallelism setting for this task, which is heavier (20 slots)
    mobile_web_popular_pages = \
        DockerBashOperator(task_id='MobileWebPopularPages',
                           bash_command='''web/popular_pages.sh -d {{ ds }}'''
                           )
    mobile_web_popular_pages.set_upstream(prepare_hbase_tables)

    sum_ww_all = DummyOperator(task_id='SumWWAll',
                               dag=dag
                               )
    days_to_compute_back = 31
    if is_window_dag():
        days_to_compute_back = int(WINDOW_MODE_TYPE.split('-')[1])

    for i in range(0, days_to_compute_back):

        def branching_logic(**kwargs):
            task = kwargs['task']
            i = kwargs['params']['i']
            is_valid_day = \
                task.render_template('''{{ macros.dss_in_same_month(ds,macros.ds_add(ds,-%s))}}''' % i,
                                     kwargs) == 'True'
            branch = 'SumWwDay_DT-%s' % i if is_valid_day else 'SumWwDay_DT-%s_Sentinel' % i
            return branch

        if is_snapshot_dag():
            sum_ww_day_i_check = \
                BranchPythonOperator(
                        task_id='SumWwDay_DT-%s_Check' % i,
                        dag=dag,
                        provide_context=True,
                        params={'i': i},
                        python_callable=branching_logic
                )

            sum_ww_day_i_sentinel = \
                DummyOperator(task_id='SumWwDay_DT-%s_Sentinel' % i,
                              dag=dag
                              )

            sum_ww_day_i_done = DummyOperator(task_id='SumWwDay_DT-%s_Done' % i,
                                              dag=dag,
                                              trigger_rule='one_success'
                                              )

        sum_ww_day_i = \
            DockerBashOperator(task_id='SumWwDay_DT-%s' % i,
                               bash_command='''web/popular_pages.sh -d {{ macros.ds_add(ds,-1) }} -env main -m daily -mt {{ params.mode_type }} -x {{ macros.dss_in_same_month(ds, macros.ds_add(ds,-%s)) }} ''' % i
                               )

        if is_snapshot_dag():
            sum_ww_day_i_check.set_upstream(adjust_calc)
            sum_ww_day_i.set_upstream(sum_ww_day_i_check)
            sum_ww_day_i_sentinel.set_upstream(sum_ww_day_i_check)
            sum_ww_day_i_done.set_upstream(sum_ww_day_i)
            sum_ww_day_i_done.set_upstream(sum_ww_day_i_sentinel)
            sum_ww_all.set_upstream(sum_ww_day_i_done)
        else:
            sum_ww_day_i.set_upstream(adjust_calc)
            sum_ww_all.set_upstream(sum_ww_day_i)

    # TODO configure parallelism setting for this task, which is heavier (20 slots)
    mobile_web_adjust_store = \
        DockerBashOperator(task_id='MobileWebAdjustStore',
                           bash_command='''web/popular_pages.sh -d {{ ds }} -env main -p store'''
                           )
    mobile_web_adjust_store.set_upstream(sum_ww_all)

    mobile_web_pre = DummyOperator(task_id='MobileWebPre',
                                   dag=dag
                                   )
    if is_snapshot_dag():
        mobile_web_pre.set_upstream([mobile_web_compare_est_to_qc, adjust_calc, mobile_web_calc_subdomains,
                                     mobile_web_popular_pages, mobile_web_predict_validate_preparation,
                                     mobile_web_model_validate, mobile_web_predict_validate,
                                     mobile_web_check_daily_estimations])
    else:
        mobile_web_pre.set_upstream([adjust_calc, mobile_web_calc_subdomains, mobile_web_popular_pages,
                                     mobile_web_check_daily_estimations])

    mobile_web = DummyOperator(task_id='MobileWeb',
                               dag=dag
                               )

    mobile_web.set_upstream([sum_ww_all, mobile_web_pre, mobile_web_adjust_store])

    return dag


generate_dags(SNAPHOT_MODE)
generate_dags(WINDOW_MODE)
