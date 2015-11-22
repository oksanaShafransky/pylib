__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator
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
    'start_date': datetime(15, 11, 10),
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}


def generate_dags(mode):
    def is_window_dag():
        return mode == WINDOW_MODE

    def is_snapshot_dag():
        return mode == SNAPHOT_MODE

    #TODO insert the real logic here
    def is_prod_env():
        return True

    dag_template_params_for_mode = dag_template_params.copy()
    mode_dag_template_params = {}

    if is_window_dag():
        mode_dag_template_params = {'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE}

    if is_snapshot_dag():
        mode_dag_template_params = {'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE}

    dag_template_params_for_mode.update(mode_dag_template_params)

    dag = DAG(dag_id='MobileAppsMovingWindow_' + mode, default_args=dag_args, params=dag_template_params_for_mode,
              schedule_interval=timedelta(days=1))

    mobile_daily_estimation = ExternalTaskSensor(external_dag_id='MobileDailyEstimation',
                                                 dag=dag,
                                                 task_id="MobileDailyEstimation",
                                                 external_task_id='FinishProcess')

    ########################
    # Prepare HBase Tables #
    ########################

    prepare_hbase_tables = \
        DockerBashOperator(task_id='PrepareHBaseTables',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/start-process.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p tables'''
                           )

    prepare_hbase_tables.set_upstream(mobile_daily_estimation)

    #####################
    # App Usage Pattern #
    #####################

    usage_pattern_calculation = \
        DockerBashOperator(task_id='AppsUsagePatternCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calculation'''
                           )

    usage_pattern_calculation.set_upstream(mobile_daily_estimation)

    app_usage_pattern_store = \
        DockerBashOperator(task_id='AppsUsagePatternStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p app_store'''
                           )

    app_usage_pattern_store.set_upstream([usage_pattern_calculation,
                                          prepare_hbase_tables])

    usage_raw_totals = \
        DockerBashOperator(task_id='UsageRawTotals',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p raw_totals'''
                           )

    # dependency on usage_store is important since this job writes to the same cf and usage_calc checks if it is populated
    # so inversing this order may cause usage_calc to decide not to run
    usage_raw_totals.set_upstream(app_usage_pattern_store)

    category_usage = \
        DockerBashOperator(task_id='CategoryUsage',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p category_store'''
                           )

    category_usage.set_upstream([prepare_hbase_tables,
                                 usage_pattern_calculation])

    usage_pattern_category_leaders = \
        DockerBashOperator(task_id='UsagePatternCategoryLeaders',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p category_leaders'''
                           )

    usage_pattern_category_leaders.set_upstream([prepare_hbase_tables,
                                                 app_usage_pattern_store,
                                                 usage_raw_totals])

    #################
    # App Retention #
    #################

    app_retention_calculation = \
        DockerBashOperator(task_id='AppRetentionCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calc_apps'''
                           )

    app_retention_calculation.set_upstream(mobile_daily_estimation)

    app_churn_calculation = \
        DockerBashOperator(task_id='AppChurnCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p churn_calc'''
                           )

    app_churn_calculation.set_upstream(app_retention_calculation)

    smooth_app_retention = \
        DockerBashOperator(task_id='SmoothAppRetention',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p smooth_retention'''
                           )

    smooth_app_retention.set_upstream(app_churn_calculation)

    app_retention_precalculation = DummyOperator(task_id='AppRetentionPrecalculation',
                                                 dag=dag
                                                 )

    app_retention_precalculation.set_upstream(smooth_app_retention)

    if is_window_dag():
        check_app_and_country_retention_estimation = \
            DockerBashOperator(task_id='CheckAppAndCountryRetentionEstimation',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/qa/app-retention/qa/checkAppAndCountryRetentionEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p check'''
                               )
        check_app_and_country_retention_estimation.set_upstream(app_retention_precalculation)

    app_retention_categories_calculation = \
        DockerBashOperator(task_id='AppRetentionCategoriesCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calc_cats'''
                           )

    app_retention_categories_calculation.set_upstream(app_retention_precalculation)

    app_retention_aggregate_categories = \
        DockerBashOperator(task_id='AppRetentionAggregateCategories',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p agg_cats'''
                           )

    app_retention_aggregate_categories.set_upstream(app_retention_categories_calculation)

    app_retention_categories = DummyOperator(task_id='AppRetentionCategories',
                                             dag=dag
                                             )
    app_retention_categories.set_upstream(app_retention_aggregate_categories)

    app_retention_store = \
        DockerBashOperator(task_id='AppRetentionStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p store_apps'''
                           )

    app_retention_store.set_upstream([prepare_hbase_tables, app_retention_precalculation])

    category_retention_store = \
        DockerBashOperator(task_id='CategoryRetentionStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p store_cats'''
                           )

    category_retention_store.set_upstream([prepare_hbase_tables, app_retention_categories])

    retention_store = DummyOperator(task_id='RetentionStore',
                                    dag=dag
                                    )
    retention_store.set_upstream([app_retention_store, category_retention_store])

    retention_leaders_calculation = \
        DockerBashOperator(task_id='RetentionLeadersCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p leaders_calc'''
                           )

    retention_leaders_calculation.set_upstream([prepare_hbase_tables, retention_store])

    retention_leaders_store = \
        DockerBashOperator(task_id='RetentionLeadersStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p leaders_store'''
                           )

    retention_leaders_store.set_upstream(retention_leaders_calculation)

    retention_leaders = DummyOperator(task_id='RetentionLeaders',
                                      dag=dag
                                      )
    retention_leaders.set_upstream(retention_leaders_store)

    ########################
    # Application Affinity #
    ########################

    if is_window_dag():
        affinity_country_filter = '-c ' + EtcdHook().get_record(ETCD_ENV_ROOT['PRODUCTION'],
                                                                'services/app-affinity/window/countries')
    else:
        affinity_country_filter = ''

    dag_template_params_for_mode.update({'affinity_country_filter': affinity_country_filter})

    # TODO configure parallelsim setting for this task, which is heavier (5 slots)
    app_affinity_app_precalculation = \
        DockerBashOperator(task_id='AppAffinityAppPrecalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-affinity/affinity.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} {{ params.affinity_country_filter }} -p app_panel_preparation'''
                           )

    app_affinity_app_precalculation.set_upstream(mobile_daily_estimation)

    # TODO configure parallelsim setting for this task, which is heavier (5 slots)
    app_affinity_country_precalculation = \
        DockerBashOperator(task_id='AppAffinityCountryPrecalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-affinity/affinity.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} {{ params.affinity_country_filter }} -p country_panel_preparation'''
                           )

    app_affinity_country_precalculation.set_upstream(mobile_daily_estimation)

    app_affinity_precalculation = DummyOperator(task_id='AppAffinityPrecalculation',
                                                dag=dag
                                                )
    app_affinity_precalculation.set_upstream([app_affinity_app_precalculation, app_affinity_country_precalculation])

    # TODO configure parallelsim setting for this task, which is heavier (20 slots)
    app_affinity_pairs = \
        DockerBashOperator(task_id='AppAffinityPairs',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-affinity/affinity.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} {{ params.affinity_country_filter }} -p calc_affinity'''
                           )

    app_affinity_pairs.set_upstream(app_affinity_precalculation)

    app_affinity_store = \
        DockerBashOperator(task_id='AppAffinityStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-affinity/affinity.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p store_affinity'''
                           )

    app_affinity_store.set_upstream([prepare_hbase_tables, app_affinity_pairs])

    if is_window_dag():
        affinity_sanity_check = \
            DockerBashOperator(task_id='AffinitySanityCheck',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-affinity/qa/checkAppAndCountryAffinityEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p check'''
                               )
        affinity_sanity_check.set_upstream(app_affinity_pairs)

    app_affinity = DummyOperator(task_id='AppAffinity',
                                 dag=dag
                                 )
    app_affinity.set_upstream(app_affinity_store)

    ##############
    # Mobile Web #
    ##############

    mobile_web_train_model = None
    if is_snapshot_dag():
        # TODO configure parallelsim setting for this task, which is heavier (20 slots)
        mobile_web_predict_validate_preparation = \
            DockerBashOperator(task_id='MobileWebPredictValidatePreparation',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/web/second_stage_tests.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -wenv daily-cut -m {{ params.mode }} -mt {{ params.mode_type }} -p prepare_total_device_count'''
                               )
        mobile_web_predict_validate_preparation.set_upstream(mobile_daily_estimation)

        # TODO configure parallelsim setting for this task, which is heavier (20 slots)
        mobile_web_predict_validate = \
            DockerBashOperator(task_id='MobileWebPredictValidate',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/web/second_stage_tests.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -wenv daily-cut -m {{ params.mode }} -mt {{ params.mode_type }} -p prepare_predictions_for_test,verify_predictions'''
                               )

        # TODO add dependency on mobile_web_adjust_calc
        mobile_web_predict_validate.set_upstream([mobile_web_predict_validate_preparation])

        # TODO configure parallelsim setting for this task, which is heavier (20 slots)
        mobile_web_compare_est_to_qc = \
            DockerBashOperator(task_id='MobileWebCompareEstToQC',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/web/compare_estimations_to_qc.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -sm -env main -m {{ params.mode }} -mt {{ params.mode_type }}'''
                               )

        # TODO configure parallelsim setting for this task, which is heavier (20 slots)
        mobile_web_train_model = \
            DockerBashOperator(task_id='MobileWebTrainModel',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/web/train_mobile_web_model.sh -d {{ ds }} -fd {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }}'''
                               )

        # TODO configure parallelsim setting for this task, which is heavier (20 slots)
        mobile_web_model_validate = \
            DockerBashOperator(task_id='MobileWebModelValidate',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/web/second_stage_tests.sh -d {{ ds }} -fd {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -wenv daily-cut -m {{ params.mode }} -mt {{ params.mode_type }} -p check_model'''
                               )
        mobile_web_model_validate.set_upstream(mobile_web_train_model)

    mobile_web_gaps_filler = \
        DockerBashOperator(task_id='MobileWebGapsFiller',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/web/mobile_web_gaps_filler.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )
    mobile_web_gaps_filler.set_upstream(mobile_daily_estimation)

    # TODO I should verify: is the task ID right? should we concatenate the date?
    # TODO configure parallelsim setting for this task, which is heavier (10 slots)
    mobile_web_first_stage_agg = \
        DockerBashOperator(task_id='MobileWebFirstStageAgg',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/web/first_stage_agg.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m window -mt last-28'''
                           )
    if is_snapshot_dag():
        mobile_web_train_model.set_upstream(mobile_web_first_stage_agg)
        mobile_web_compare_est_to_qc.set_upstream(mobile_web_first_stage_agg)

    # TODO configure parallelsim setting for this task, which is heavier (10 slots)
    mobile_web_first_stage_agg.set_upstream(mobile_daily_estimation)

    # TODO I should verify: is the task ID right? should we concatenate the date?
    # TODO configure parallelsim setting for this task, which is heavier (10 slots)
    mobile_web_adjust_calc_intermediate = \
        DockerBashOperator(task_id='MobileWebAdjustCalcIntermediate',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/web/adjust_est.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -wenv daily-cut -m window -mt last-28 -p prepare_data,predict'''
                           )
    if is_snapshot_dag():
        mobile_web_adjust_calc_intermediate.set_upstream([mobile_web_train_model, mobile_web_first_stage_agg])
    else:
        mobile_web_adjust_calc_intermediate.set_upstream(mobile_web_first_stage_agg)

    # TODO configure parallelsim setting for this task, which is heavier (20 slots)
    mobile_web_adjust_calc = \
        DockerBashOperator(task_id='MobileWebAdjustCalc',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/web/adjust_est.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -wenv daily-cut -m window -mt last-28 -p redist'''
                           )
    mobile_web_adjust_calc.set_upstream([mobile_web_gaps_filler,mobile_web_adjust_calc_intermediate])

    # TODO configure parallelsim setting for this task, which is heavier (20 slots)
    mobile_web_check_daily_estimations = \
        DockerBashOperator(task_id='MobileWebCheckDailyEstimations',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/web/check_daily_estimations.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -p redist'''
                           )
    mobile_web_check_daily_estimations.set_upstream(mobile_web_adjust_calc)

    # TODO configure parallelsim setting for this task, which is heavier (20 slots)
    mobile_web_calc_subdomains = \
        DockerBashOperator(task_id='MobileWebCalcSubdomains',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/web/calc_subdomains.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )
    mobile_web_calc_subdomains.set_upstream(mobile_web_adjust_calc)

    # TODO configure parallelsim setting for this task, which is heavier (20 slots)
    mobile_web_popular_pages = \
        DockerBashOperator(task_id='MobileWebPopularPages',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/web/popular_pages.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
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
            i=kwargs['params']['i']
            is_valid_day = \
                task.render_template('''{{ macros.dss_in_same_month(ds,macros.ds_add(ds,-%s))}}''' % i,
                                     kwargs)=='True'
            branch ='SumWwDay_DT-%s' % i if is_valid_day else 'SumWwDay_DT-%s_Sentinel' % i
            return branch

        if is_snapshot_dag():

            sum_ww_day_i_check = \
                BranchPythonOperator(
                    task_id='SumWwDay_DT-%s_Check' % i,
                    dag=dag,
                    provide_context=True,
                    params={'i': i},
                    python_callable= branching_logic
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
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/web/popular_pages.sh -d {{ macros.ds_add(ds,-1) }} -bd {{ params.base_hdfs_dir }} -env main -m daily -mt {{ params.mode_type }} -x {{ macros.dss_in_same_month(ds, macros.ds_add(ds,-%s)) }} ''' % i
                               )

        if is_snapshot_dag():
            sum_ww_day_i_check.set_upstream(mobile_web_adjust_calc)
            sum_ww_day_i.set_upstream(sum_ww_day_i_check)
            sum_ww_day_i_sentinel.set_upstream(sum_ww_day_i_check)
            sum_ww_day_i_done.set_upstream(sum_ww_day_i)
            sum_ww_day_i_done.set_upstream(sum_ww_day_i_sentinel)
            sum_ww_all.set_upstream(sum_ww_day_i_done)
        else:
            sum_ww_day_i.set_upstream(mobile_web_adjust_calc)
            sum_ww_all.set_upstream(sum_ww_day_i)

    # TODO configure parallelsim setting for this task, which is heavier (20 slots)
    mobile_web_adjust_store = \
        DockerBashOperator(task_id='MobileWebAdjustStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/web/popular_pages.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }} -p store'''
                           )
    mobile_web_adjust_store.set_upstream(sum_ww_all)

    mobile_web_pre = DummyOperator(task_id='MobileWebPre',
                               dag=dag
                               )
    if is_snapshot_dag():
        mobile_web_pre.set_upstream([mobile_web_compare_est_to_qc,mobile_web_adjust_calc,mobile_web_calc_subdomains,mobile_web_popular_pages,mobile_web_predict_validate_preparation,mobile_web_model_validate,mobile_web_predict_validate,mobile_web_check_daily_estimations])
    else:
        mobile_web_pre.set_upstream([mobile_web_adjust_calc,mobile_web_calc_subdomains,mobile_web_popular_pages,mobile_web_check_daily_estimations])


    mobile_web = DummyOperator(task_id='MobileWeb',
                                   dag=dag
                                   )

    mobile_web.set_upstream([sum_ww_all,mobile_web_pre,mobile_web_adjust_store])

    ##################
    # App Engagement #
    ##################

    app_engagement = \
        DockerBashOperator(task_id='AppEngagement',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/engagement.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )
    app_engagement.set_upstream(prepare_hbase_tables)

    if is_window_dag():
        app_engagement_sanity_check = \
            DockerBashOperator(task_id='AppEngagementSanityCheck',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/qa/checkAppAndCountryEngagementEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main  -m {{ params.mode }} -mt {{ params.mode_type }} -p check_window'''
                               )
        app_engagement_sanity_check.set_upstream(app_engagement)


    #############
    # App Ranks #
    #############

    daily_app_ranks_precalculation = ExternalTaskSensor(external_dag_id='DailyAppRanksPrecalculation',
                                                 dag=dag,
                                                 task_id="DailyAppRanksPrecalculation",
                                                 external_task_id='DailyAppRanksSuppl')

    usage_ranks_main = \
        DockerBashOperator(task_id='UsageRanksMain',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/ranks.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )
    usage_ranks_main.set_upstream([daily_app_ranks_precalculation,app_engagement])

    usage_ranks = DummyOperator(task_id='UsageRanks',
                              dag=dag
                              )

    usage_ranks.set_upstream(usage_ranks_main)

    prepare_ranks = \
        DockerBashOperator(task_id='PrepareRanks',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/cross_cache.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }} -p prepare_app_rank_export'''
                           )

    prepare_ranks.set_upstream(usage_ranks)

    ranks_export_stage = \
        DockerBashOperator(task_id='RanksExportStage',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/cross_cache.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }} -et STAGE -p export'''
                           )

    ranks_export_stage.set_upstream(prepare_ranks)

    #TODO add check that this is indeed prod environment
    if is_prod_env():
        ranks_export_prod = \
            DockerBashOperator(task_id='RanksExportProd',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/cross_cache.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }} -et PRODUCTION -p export'''
                               )

        ranks_export_prod.set_upstream(prepare_ranks)

    export_ranks = DummyOperator(task_id='ExportRanks',
                               dag=dag
                               )

    if is_prod_env():
        export_ranks.set_upstream([ranks_export_prod,ranks_export_stage,prepare_ranks])
    else:
        export_ranks.set_upstream([ranks_export_stage,prepare_ranks])


    ##########
    # Trends #
    ##########

    trends = DummyOperator(task_id='Trends',
                                 dag=dag
                                 )

    if is_window_dag():
        # TODO configure parallelsim setting for this task, which is heavier (20 slots)
        trends_28_days = \
            DockerBashOperator(task_id='Trends28Days',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/trends.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -td 28'''
                               )
        trends_28_days.set_upstream([usage_ranks])

        # TODO configure parallelsim setting for this task, which is heavier (20 slots)
        trends_7_days = \
            DockerBashOperator(task_id='Trends7Days',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/trends.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -td 7'''
                               )
        trends_7_days.set_upstream([usage_ranks])

        trends.set_upstream([trends_28_days,trends_7_days])

    if is_snapshot_dag():
        trends_1_month = \
            DockerBashOperator(task_id='Trends1Month',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/trends.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -td 1'''
                               )
        trends_1_month.set_upstream([usage_ranks])

        trends.set_upstream(trends_1_month)


    ###########################
    # Copy to Prod Mobile Web #
    ###########################

    copy_to_prod_mobile_web = DummyOperator(task_id='CopyToProdMobileWeb',
                                            dag=dag
                                            )

    #TODO check why is it configured on local docker
    if is_window_dag():
        copy_to_prod_mobile_web_hbp1_window = \
            DockerBashOperator(task_id='CopyToProdMobileWebHbp1',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''hbasecopy mrp hbp1 mobile_web_stats_{{ params.mode_type }}_{{ds_format(ds, "%Y-%m-%d", "%yy_%mm-%dd")}}'''
                               )
        copy_to_prod_mobile_web.set_upstream(copy_to_prod_mobile_web_hbp1_window)
        copy_to_prod_mobile_web_hbp1_window.set_upstream(mobile_web)

    #TODO check why is it configured on local docker
    if is_snapshot_dag():
        copy_to_prod_mobile_web_hbp1_snapshot = \
            DockerBashOperator(task_id='CopyToProdMobileWebHbp1',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''hbasecopy mrp hbp1 mobile_web_stats_{{ds_format(ds, "%Y-%m-", "%yy_%mm-%dd")}}'''
                               )
        copy_to_prod_mobile_web.set_upstream(copy_to_prod_mobile_web_hbp1_snapshot)
        copy_to_prod_mobile_web_hbp1_snapshot.set_upstream(mobile_web)


    ####################
    # Dynamic Settings #
    ####################


    apps = DummyOperator(task_id='Apps',
                         dag=dag
                         )
    apps.set_upstream([app_engagement,app_affinity,retention_store,app_retention_categories,retention_leaders,app_usage_pattern_store,category_usage,usage_pattern_category_leaders,usage_ranks,export_ranks,trends])


    update_dynamic_settings = \
        DockerBashOperator(task_id='UpdateDynamicSettings',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et STAGE -p apps_sdk,apps_cross'''
                           )
    update_dynamic_settings.set_upstream(apps)

    ###############################
    # Dynamic Settings Mobile Web #
    ###############################

    update_dynamic_settings_mobile_web = \
        DockerBashOperator(task_id='UpdateDynamicSettingsMobileWeb',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et STAGE -p mobile_web'''
                           )
    update_dynamic_settings_mobile_web.set_upstream(mobile_web)

    if is_window_dag():
        update_dynamic_settings_prod_mobile_web = \
            DockerBashOperator(task_id='UpdateDynamicSettingsProdMobileWeb',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et PRODUCTION -p mobile_web'''
                               )
        update_dynamic_settings_prod_mobile_web.set_upstream([mobile_web,copy_to_prod_mobile_web])


    #######################
    # Top Apps for Sanity #
    #######################

    if is_snapshot_dag():
        top_apps_for_sanity = \
            DockerBashOperator(task_id='TopAppsForSanity',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/qaTopApps.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }}'''
                               )

        top_apps_for_sanity.set_upstream(usage_ranks)

    #######################
    # Apps Clean-Up Stage #
    #######################

    apps_cleanup_stage = DummyOperator(task_id='AppsCleanupStage',
                                       dag=dag
                                       )

    if is_window_dag():
        for i in range(3,8):
            apps_cleanup_stage_dt_minus_i = \
                DockerBashOperator(task_id='AppsCleanupStage_DT-%s' % i,
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/mobile/scripts/windowCleanup-settings.sh -d {{ ds_add(ds,-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p delete_files -p drop_hbase_tables''' % i
                                   )
            apps_cleanup_stage_dt_minus_i.set_upstream(apps)
            apps_cleanup_stage.set_upstream(apps_cleanup_stage_dt_minus_i)

    ############################
    # Local Availability Dates #
    ############################



    return dag




globals()['dag_apps_moving_window_snapshot'] = generate_dags(SNAPHOT_MODE)
globals()['dag_apps_moving_window_daily'] = generate_dags(WINDOW_MODE)
