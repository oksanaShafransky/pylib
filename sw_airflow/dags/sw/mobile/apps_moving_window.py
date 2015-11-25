__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator
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
DEFAULT_HBASE_CLUSTER = 'mrp-hbp1'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION',
                       'cluster': DEFAULT_CLUSTER, 'hbase_cluster': DEFAULT_HBASE_CLUSTER}


def generate_dags(mode):
    def is_window_dag():
        return mode == WINDOW_MODE

    def is_snapshot_dag():
        return mode == SNAPHOT_MODE

    #TODO insert the real logic here
    def is_prod_env():
        return True

    if is_window_dag():
        dag_args.update({'start_date': datetime(2015, 11, 10)})

    if is_snapshot_dag():
        dag_args.update({'start_date': datetime(2015, 9, 30)})

    dag_template_params_for_mode = dag_template_params.copy()
    mode_dag_template_params = {}

    if is_window_dag():
        mode_dag_template_params = {'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE}

    if is_snapshot_dag():
        mode_dag_template_params = {'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE}

    dag_template_params_for_mode.update(mode_dag_template_params)

    dag = DAG(dag_id='MobileAppsMovingWindow_' + mode, default_args=dag_args, params=dag_template_params_for_mode,
              schedule_interval=(timedelta(days=1)) if (is_window_dag()) else '0 0 l * *')

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
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/start-process.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -fl apps -p tables'''
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

    usage_pattern_categories = \
        DockerBashOperator(task_id='CategoryUsage',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p category_store'''
                           )

    usage_pattern_categories.set_upstream([prepare_hbase_tables,
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

    # TODO configure parallelism setting for this task, which is heavier (5 slots)
    app_affinity_app_precalculation = \
        DockerBashOperator(task_id='AppAffinityAppPrecalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-affinity/affinity.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} {{ params.affinity_country_filter }} -p app_panel_preparation'''
                           )

    app_affinity_app_precalculation.set_upstream(mobile_daily_estimation)

    # TODO configure parallelism setting for this task, which is heavier (5 slots)
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

    # TODO configure parallelism setting for this task, which is heavier (20 slots)
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
        # TODO configure parallelism setting for this task, which is heavier (20 slots)
        trends_28_days = \
            DockerBashOperator(task_id='Trends28Days',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/trends.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -td 28'''
                               )
        trends_28_days.set_upstream([usage_ranks])

        # TODO configure parallelism setting for this task, which is heavier (20 slots)
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


    ####################
    # Dynamic Settings #
    ####################


    apps = DummyOperator(task_id='Apps',
                         dag=dag
                         )
    apps.set_upstream([app_engagement,app_affinity,retention_store,app_retention_categories,retention_leaders,app_usage_pattern_store,usage_pattern_categories,usage_pattern_category_leaders,usage_ranks,export_ranks,trends])


    update_dynamic_settings = \
        DockerBashOperator(task_id='UpdateDynamicSettings',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et STAGE -p apps_sdk,apps_cross'''
                           )
    update_dynamic_settings.set_upstream(apps)


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

    if is_window_dag():

        apps_cleanup_stage = DummyOperator(task_id='AppsCleanupStage',
                                           dag=dag
                                           )

        for i in range(3,8):
            apps_cleanup_stage_dt_minus_i = \
                DockerBashOperator(task_id='AppsCleanupStage_DT-%s' % i,
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/mobile/scripts/windowCleanup-settings.sh -d {{ ds_add(ds,-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -fl apps -p delete_files -p drop_hbase_tables''' % i
                                   )
            apps_cleanup_stage_dt_minus_i.set_upstream(apps)
            apps_cleanup_stage.set_upstream(apps_cleanup_stage_dt_minus_i)

    ############################
    # Local Availability Dates #
    ############################

    #TODO check why is it configured on local docker
    update_usage_ranks_date_stage = \
        DockerBashOperator(task_id='UpdateUsageRanksDateStage',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ ds_add(ds,-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et STAGE -p usage_ranks -pn UsageRanksStage -um success'''
                       )
    update_usage_ranks_date_stage.set_upstream(usage_ranks)


    ################
    # Cleanup Prod #
    ################

    if is_prod_env():
        if is_window_dag():

            cleanup_prod = DummyOperator(task_id='CleanupProd',
                                 dag=dag
                                 )

            for i in range(3,8):
                cleanup_hbp1_ds_minus_i = \
                    DockerBashOperator(task_id='CleanupHbp1_DS-%s' % i,
                                       dag=dag,
                                       docker_name='''{{ params.hbase_cluster }}''',
                                       bash_command='''{{ params.execution_dir }}/mobile/scripts/windowCleanup.sh -d {{ ds_add(ds,-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -fl apps -p drop_hbase_tables''' % i
                                       )
                cleanup_hbp1_ds_minus_i.set_upstream(apps)

                ranks_etcd_prod_cleanup_ds_minus_i = \
                    DockerBashOperator(task_id='RanksEtcdProdCleanup_DS-%s' % i,
                                       dag=dag,
                                       docker_name='''{{ params.hbase_cluster }}''',
                                       bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ ds_add(ds,-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et PRODUCTION -p usage_ranks -pn UsageRanksProd -um failure''' % i
                                       )
                ranks_etcd_prod_cleanup_ds_minus_i.set_upstream(cleanup_hbp1_ds_minus_i)
                cleanup_prod.set_upstream(ranks_etcd_prod_cleanup_ds_minus_i)

    #####################
    # Copy to Prod Apps #
    #####################

    hbase_suffix_template = ('''{{ params.mode_type }}_{{ macros.ds_format(ds, "%Y-%m-%d", "%y_%m_%d")}}''' if is_window_dag() else
                        '''{{macros.ds_format(ds, "%Y-%m-%d", "%y_%m")}}''');

    if is_prod_env():
        # TODO configure parallelism setting for this task, which is heavier (30 slots)
        copy_to_prod_app_sdk_hbp1 = \
            DockerBashOperator(task_id='CopyToProdAppSdkHbp1',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='hbasecopy mrp hbp1 app_sdk_stats_' + hbase_suffix_template
                           )

        copy_to_prod_app_sdk_hbp1.set_upstream([app_engagement,app_affinity,retention_store,app_usage_pattern_store])

        # TODO configure parallelism setting for this task, which is heavier (30 slots)
        copy_to_prod_cats_hbp1 = \
            DockerBashOperator(task_id='CopyToProdCatsHbp1',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='hbasecopy mrp hbp1 app_sdk_category_stats_' + hbase_suffix_template
                               )

        copy_to_prod_cats_hbp1.set_upstream([app_engagement,category_retention_store,usage_pattern_categories])

        # TODO configure parallelism setting for this task, which is heavier (30 slots)
        copy_to_prod_leaders_hbp1 = \
            DockerBashOperator(task_id='CopyToProdLeadersHbp1',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='hbasecopy mrp hbp1 app_sdk_category_lead_' + hbase_suffix_template
                               )

        copy_to_prod_leaders_hbp1.set_upstream([app_engagement,retention_leaders,usage_pattern_category_leaders])

        # TODO configure parallelism setting for this task, which is heavier (30 slots)
        copy_to_prod_engage_hbp1 = \
            DockerBashOperator(task_id='CopyToProdEngageHbp1',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='hbasecopy mrp hbp1 app_eng_rank_' + hbase_suffix_template
                               )

        copy_to_prod_engage_hbp1.set_upstream(usage_ranks)

        # TODO configure parallelism setting for this task, which is heavier (30 slots)
        copy_to_prod_rank_hbp1 = \
            DockerBashOperator(task_id='CopyToProdRankHbp1',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='hbasecopy mrp hbp1 cat_mod_app_rank_' + hbase_suffix_template
                               )

        copy_to_prod_rank_hbp1.set_upstream([usage_ranks,trends])

        copy_to_prod_apps = DummyOperator(task_id='CopyToProdApps',
                             dag=dag
                             )

        copy_to_prod_apps.set_upstream([copy_to_prod_app_sdk_hbp1,copy_to_prod_cats_hbp1,copy_to_prod_leaders_hbp1,
                                        copy_to_prod_engage_hbp1,copy_to_prod_rank_hbp1])


    #########################
    # Dynamic Settings Apps #
    #########################

    if is_prod_env():
        if is_window_dag():
            update_dyn_set_apps_prod = \
                DockerBashOperator(task_id='UpdateDynSetAppsProd',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et PRODUCTION_MRP'''
                                   )

            update_dyn_set_apps_prod.set_upstream([copy_to_prod_apps,apps])


    #############################
    # Update Availability Dates #
    #############################

    if is_prod_env():
        #TODO check why is it configured on local docker
        update_usage_ranks_date_prod = \
            DockerBashOperator(task_id='UpdateUsageRanksDateProd',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et PRODUCTION_MRP -p usage_ranks -pn UsageRanksProd -um success'''
                               )

        update_usage_ranks_date_prod.set_upstream(copy_to_prod_rank_hbp1)

    return dag


globals()['dag_apps_moving_window_snapshot'] = generate_dags(SNAPHOT_MODE)
globals()['dag_apps_moving_window_daily'] = generate_dags(WINDOW_MODE)
