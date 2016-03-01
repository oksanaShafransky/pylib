
__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from sw.airflow.docker_bash_operator import DockerBashOperator
from sw.airflow.key_value import *
from sw.airflow.external_sensors import AdaptedExternalTaskSensor, AggRangeExternalTaskSensor
from sw.airflow.operators import DockerCopyHbaseTableOperator
from airflow.models import Variable

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
WINDOW_MODE = 'window'
SNAPHOT_MODE = 'snapshot'
WINDOW_MODE_TYPE = 'last-28'
SNAPSHOT_MODE_TYPE = 'monthly'
DEFAULT_HBASE_CLUSTER = 'hbp1'
IS_PROD = True

dag_args = {
    'owner': 'similarweb',
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com', 'n7i6d2a2m1h2l3f6@similar.slack.com', 'airflow@similarweb.pagerduty.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION',
                       'cluster': DEFAULT_CLUSTER, 'hbase_cluster': DEFAULT_HBASE_CLUSTER}


def generate_dag(mode):
    def is_window_dag():
        return mode == WINDOW_MODE

    def is_snapshot_dag():
        return mode == SNAPHOT_MODE

    def mode_dag_name():
        if is_window_dag():
            return 'Window'
        if is_snapshot_dag():
            return 'Snapshot'

    # TODO insert the real logic here
    def is_prod_env():
        return IS_PROD

    dag_args_for_mode = dag_args.copy()
    if is_window_dag():
        dag_args_for_mode.update({'start_date': datetime(2016, 1, 21)})

    if is_snapshot_dag():
        dag_args_for_mode.update({'start_date': datetime(2016, 1, 1), 'end_date': datetime(2016, 1, 1)})

    dag_template_params_for_mode = dag_template_params.copy()
    if is_window_dag():
        dag_template_params_for_mode.update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})

    if is_snapshot_dag():
        dag_template_params_for_mode.update({'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

    dag = DAG(dag_id='AndroidApps_' + mode_dag_name(), default_args=dag_args_for_mode,
              params=dag_template_params_for_mode,
              schedule_interval="@daily" if is_window_dag() else "@monthly")

    mobile_estimation = AggRangeExternalTaskSensor(external_dag_id='AndroidApps_Estimation',
                                                   dag=dag,
                                                   task_id='Estimation',
                                                   external_task_id='Estimation',
                                                   agg_mode='''{{ params.mode_type }}''')

    mobile_preliminary_daily_aggregation = AggRangeExternalTaskSensor(external_dag_id='Mobile_Preliminary',
                                                                      dag=dag,
                                                                      task_id='MobileDailyAggregation',
                                                                      external_task_id='DailyAggregation',
                                                                      agg_mode='''{{ params.mode_type }}''')

    ########################
    # Prepare HBase Tables #
    ########################

    prepare_hbase_tables = \
        DockerBashOperator(task_id='PrepareHBaseTables',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/start-process.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -fl APPS -p tables'''
                           )

    #####################
    # App Usage Pattern #
    #####################

    usage_pattern_calculation = \
        DockerBashOperator(task_id='AppsUsagePatternCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calculation'''
                           )

    usage_pattern_calculation.set_upstream(mobile_estimation)

    app_usage_pattern_store = \
        DockerBashOperator(task_id='AppsUsagePatternStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p app_store'''
                           )

    app_usage_pattern_store.set_upstream([usage_pattern_calculation,
                                          prepare_hbase_tables])

    usage_raw_totals = \
        DockerBashOperator(task_id='UsageRawTotals',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p raw_totals'''
                           )

    # dependency on usage_store is important since this job writes to the same cf and usage_calc checks if it is populated
    # so inversing this order may cause usage_calc to decide not to run
    usage_raw_totals.set_upstream(app_usage_pattern_store)

    usage_pattern_categories = \
        DockerBashOperator(task_id='CategoryUsage',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p category_store'''
                           )
    usage_pattern_categories.set_upstream([prepare_hbase_tables, usage_pattern_calculation])

    usage_pattern_category_leaders = \
        DockerBashOperator(task_id='UsagePatternCategoryLeaders',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p category_leaders'''
                           )

    usage_pattern_category_leaders.set_upstream([prepare_hbase_tables, app_usage_pattern_store, usage_raw_totals])

    #################
    # App Retention #
    #################

    app_retention_calculation = \
        DockerBashOperator(task_id='AppRetentionCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calc_apps'''
                           )
    app_retention_calculation.set_upstream(mobile_preliminary_daily_aggregation)

    app_churn_calculation = \
        DockerBashOperator(task_id='AppChurnCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p churn_calc'''
                           )
    app_churn_calculation.set_upstream(app_retention_calculation)

    smooth_app_retention = \
        DockerBashOperator(task_id='SmoothAppRetention',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p smooth_retention'''
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
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/qa/checkAppAndCountryRetentionEstimation.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p check'''
                               )
        check_app_and_country_retention_estimation.set_upstream(app_retention_precalculation)

    app_retention_categories_calculation = \
        DockerBashOperator(task_id='AppRetentionCategoriesCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calc_cats'''
                           )
    app_retention_categories_calculation.set_upstream(app_retention_precalculation)

    app_retention_aggregate_categories = \
        DockerBashOperator(task_id='AppRetentionAggregateCategories',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p agg_cats'''
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
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p store_apps'''
                           )

    app_retention_store.set_upstream([prepare_hbase_tables, app_retention_precalculation])

    category_retention_store = \
        DockerBashOperator(task_id='CategoryRetentionStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p store_cats'''
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
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p leaders_calc'''
                           )
    retention_leaders_calculation.set_upstream([prepare_hbase_tables, retention_store])

    retention_leaders_store = \
        DockerBashOperator(task_id='RetentionLeadersStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p leaders_store'''
                           )
    retention_leaders_store.set_upstream(retention_leaders_calculation)

    retention_leaders = DummyOperator(task_id='RetentionLeaders',
                                      dag=dag
                                      )
    retention_leaders.set_upstream(retention_leaders_store)

    ########################
    # Application Affinity #
    ########################

    # TODO configure parallelism setting for this task, which is heavier (5 slots)
    app_affinity_app_precalculation = \
        DockerBashOperator(task_id='AppAffinityAppPrecalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-affinity/affinity.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} {{ params.affinity_country_filter }} -p app_panel_preparation'''
                           )
    app_affinity_app_precalculation.set_upstream(mobile_preliminary_daily_aggregation)

    # TODO configure parallelism setting for this task, which is heavier (5 slots)
    app_affinity_country_precalculation = \
        DockerBashOperator(task_id='AppAffinityCountryPrecalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-affinity/affinity.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} {{ params.affinity_country_filter }} -p country_panel_preparation'''
                           )
    app_affinity_country_precalculation.set_upstream(mobile_preliminary_daily_aggregation)

    app_affinity_precalculation = DummyOperator(task_id='AppAffinityPrecalculation',
                                                dag=dag
                                                )
    app_affinity_precalculation.set_upstream([app_affinity_app_precalculation, app_affinity_country_precalculation])

    # TODO configure parallelism setting for this task, which is heavier (20 slots)
    app_affinity_pairs = \
        DockerBashOperator(task_id='AppAffinityPairs',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-affinity/affinity.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} {{ params.affinity_country_filter }} -p calc_affinity'''
                           )
    app_affinity_pairs.set_upstream(app_affinity_precalculation)

    app_affinity_store = \
        DockerBashOperator(task_id='AppAffinityStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-affinity/affinity.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p store_affinity'''
                           )
    app_affinity_store.set_upstream([prepare_hbase_tables, app_affinity_pairs])

    if is_window_dag():
        affinity_sanity_check = \
            DockerBashOperator(task_id='AffinitySanityCheck',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-affinity/qa/checkAppAndCountryAffinityEstimation.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p check'''
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
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/engagement.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -env all_countries -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )
    app_engagement.set_upstream([mobile_estimation, prepare_hbase_tables])

    # Todo: Fix this task whose data was deleted fue to retention by fallbacking into using snapshot's data
    # if is_window_dag():
    #    app_engagement_sanity_check = \
    #        DockerBashOperator(task_id='AppEngagementSanityCheck',
    #                           dag=dag,
    #                           docker_name='''{{ params.cluster }}''',
    #                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/qa/checkAppAndCountryEngagementEstimation.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -env all_countries -p check_window'''
    #                           )
    #    app_engagement_sanity_check.set_upstream(app_engagement)


    #############
    # App Ranks #
    #############

    daily_ranks_backfill = AdaptedExternalTaskSensor(external_dag_id='AndroidApps_DailyRanksBackfill',
                                                     dag=dag,
                                                     task_id='DailyRanksBackfill',
                                                     external_task_id='DailyRanksBackfill',
                                                     external_execution_date='''{{ macros.last_interval_day(ds, dag.schedule_interval) }}'''
                                                     )
    calc_ranks = \
        DockerBashOperator(task_id='CalculateUsageRanks',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -env all_countries -m {{ params.mode }} -mt {{ params.mode_type }} -fs -p join_scores_info,cat_ranks'''
                           )
    calc_ranks.set_upstream([daily_ranks_backfill, app_engagement])

    usage_ranks = DummyOperator(task_id='UsageRanks',
                                dag=dag
                                )

    store_usage_ranks = \
        DockerBashOperator(task_id='StoreUsageRanks',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -env all_countries -m {{ params.mode }} -mt {{ params.mode_type }} -p store_cat_ranks''',
                           depends_on_past=True if is_snapshot_dag() else False  # In the window case - it is taken care by the daily_ranks_backfill DAG
                           )
    store_usage_ranks.set_upstream(calc_ranks)

    app_ranks_histogram_store = \
        DockerBashOperator(task_id='RecordAppUsageRanksHistory',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -env all_countries -m {{ params.mode }} -mt {{ params.mode_type }} -p store_app_ranks'''
                           )
    app_ranks_histogram_store.set_upstream(calc_ranks)

    usage_ranks.set_upstream([calc_ranks, store_usage_ranks, app_ranks_histogram_store])

    prepare_ranks = \
        DockerBashOperator(task_id='PrepareRanks',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/cross_cache.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -env all_countries -m {{ params.mode }} -mt {{ params.mode_type }} -p prepare_app_rank_export'''
                           )
    prepare_ranks.set_upstream(calc_ranks)

    ranks_export_stage = \
        DockerBashOperator(task_id='RanksExportStage',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/cross_cache.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -env all_countries -m {{ params.mode }} -mt {{ params.mode_type }} -et STAGE -p export'''
                           )
    ranks_export_stage.set_upstream(prepare_ranks)

    # TODO add check that this is indeed prod environment
    if is_prod_env():
        ranks_export_prod = \
            DockerBashOperator(task_id='RanksExportProd',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/cross_cache.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -env all_countries -m {{ params.mode }} -mt {{ params.mode_type }} -et PRODUCTION -p export'''
                               )
        ranks_export_prod.set_upstream(prepare_ranks)

    export_ranks = DummyOperator(task_id='ExportRanks',
                                 dag=dag
                                 )

    if is_prod_env():
        export_ranks.set_upstream([ranks_export_prod, ranks_export_stage, prepare_ranks])
    else:
        export_ranks.set_upstream([ranks_export_stage, prepare_ranks])

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
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/trends.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -td 28'''
                               )
        trends_28_days.set_upstream(calc_ranks)

        # TODO configure parallelism setting for this task, which is heavier (20 slots)
        trends_7_days = \
            DockerBashOperator(task_id='Trends7Days',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/trends.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -td 7'''
                               )
        trends_7_days.set_upstream([calc_ranks])
        trends.set_upstream([trends_28_days, trends_7_days])

    if is_snapshot_dag():
        trends_1_month = \
            DockerBashOperator(task_id='Trends1Month',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/trends.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -td 1'''
                               )
        trends_1_month.set_upstream(usage_ranks)
        trends.set_upstream(trends_1_month)

    apps = DummyOperator(task_id='Apps',
                         dag=dag
                         )
    apps.set_upstream([app_engagement, app_affinity, retention_store, app_retention_categories, retention_leaders,
                       app_usage_pattern_store, usage_pattern_categories, usage_pattern_category_leaders,
                       usage_ranks, export_ranks, trends])

    #######################
    # Top Apps for Sanity #
    #######################

    if is_snapshot_dag():
        top_apps_for_sanity = \
            DockerBashOperator(task_id='TopAppsForSanity',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/qaTopApps.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }}'''
                               )
        top_apps_for_sanity.set_upstream(usage_ranks)

    cleanup_from_days = 8
    cleanup_to_days = 3
    #######################
    # Apps Clean-Up Stage #
    #######################

    if is_window_dag():

        clean_id = 'StageCleanupRequested'
        cleaning_stage = DummyOperator(task_id=clean_id,
                                       dag=dag)

        cleanup_stage = DummyOperator(task_id='CleanupStage',
                                      dag=dag
                                      )

        for i in range(cleanup_to_days, cleanup_from_days):
            cleanup_stage_ds_minus_i = \
                DockerBashOperator(task_id='CleanupStage_DS-%s' % i,
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/mobile/scripts/windowCleanup.sh -d {{ macros.ds_add(ds,-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -fl APPS -p delete_files -p drop_hbase_tables''' % i,

                                   )
            cleanup_stage_ds_minus_i.set_upstream(apps)
            cleanup_stage_ds_minus_i.set_upstream(cleaning_stage)
            cleanup_stage.set_upstream(cleanup_stage_ds_minus_i)

        skip_clean_id = 'StageCleanupSkipped'
        not_cleaning_stage = DummyOperator(task_id=skip_clean_id,
                                           dag=dag)

        # for now, skip cleanup
        should_clean_stage = BranchPythonOperator(task_id='IsCleanupRequested',
                                                  dag=dag,
                                                  python_callable=lambda: clean_id)
        should_clean_stage.set_downstream([cleaning_stage, not_cleaning_stage])

    ############################
    # Local Availability Dates #
    ############################

    update_usage_ranks_date_stage = \
        DockerBashOperator(task_id='UpdateUsageRanksDateStage',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et STAGE -p usage_ranks -pn UsageRanksStage -um success'''
                           )
    update_usage_ranks_date_stage.set_upstream(usage_ranks)

    deploy_targets = Variable.get(key='hbase_deploy_targets', default_var=[], deserialize_json=True)

    ################
    # Copy to Prod #
    ################

    hbase_suffix_template = (
        '''{{ params.mode_type }}_{{ macros.ds_format(ds, "%Y-%m-%d", "%y_%m_%d")}}''' if is_window_dag() else
        '''{{macros.ds_format(ds, "%Y-%m-%d", "%y_%m")}}''')

    if is_prod_env():
        # TODO configure parallelism setting for this task, which is heavier (30 slots)
        copy_to_prod = DummyOperator(task_id='CopyToProd',
                                     dag=dag
                                     )
        copy_to_prod.set_upstream(apps)

        copy_to_prod_app_sdk = \
            DockerCopyHbaseTableOperator(
                task_id='CopyToProdAppSdk',
                dag=dag,
                docker_name='''{{ params.cluster }}''',
                source_cluster='mrp',
                target_cluster=','.join(deploy_targets),
                table_name_template='app_sdk_stats_' + hbase_suffix_template,
                is_forced=True
            )
        copy_to_prod_app_sdk.set_upstream([app_engagement, app_affinity, retention_store, app_usage_pattern_store])

        copy_to_prod_cats = \
            DockerCopyHbaseTableOperator(
                task_id='CopyToProdCats',
                dag=dag,
                docker_name='''{{ params.cluster }}''',
                source_cluster='mrp',
                target_cluster=','.join(deploy_targets),
                table_name_template='app_sdk_category_stats_' + hbase_suffix_template,
                is_forced=True
                )
        copy_to_prod_cats.set_upstream([app_engagement, category_retention_store, usage_pattern_categories])

        copy_to_prod_leaders = \
            DockerCopyHbaseTableOperator(
                task_id='CopyToProdLeaders',
                dag=dag,
                docker_name='''{{ params.cluster }}''',
                source_cluster='mrp',
                target_cluster=','.join(deploy_targets),
                table_name_template='app_sdk_category_lead_' + hbase_suffix_template,
                is_forced=True
            )
        copy_to_prod_leaders.set_upstream([app_engagement, retention_leaders, usage_pattern_category_leaders])

        copy_to_prod_engage = \
            DockerCopyHbaseTableOperator(
                task_id='CopyToProdEngage',
                dag=dag,
                docker_name='''{{ params.cluster }}''',
                source_cluster='mrp',
                target_cluster=','.join(deploy_targets),
                table_name_template='app_eng_rank_' + hbase_suffix_template,
                is_forced=True
            )
        copy_to_prod_engage.set_upstream(usage_ranks)

        copy_to_prod_rank = \
            DockerCopyHbaseTableOperator(
                task_id='CopyToProdRank',
                dag=dag,
                docker_name='''{{ params.cluster }}''',
                source_cluster='mrp',
                target_cluster=','.join(deploy_targets),
                table_name_template='cat_mod_app_rank_' + hbase_suffix_template,
                is_forced=True
            )
        copy_to_prod_rank.set_upstream([usage_ranks, trends])

        copy_to_prod.set_upstream([copy_to_prod_app_sdk, copy_to_prod_cats, copy_to_prod_leaders, copy_to_prod_engage, copy_to_prod_rank])

    ################
    # Cleanup Prod #
    ################

    if is_prod_env():
        if is_window_dag():

            cleanup_prod = DummyOperator(task_id='CleanupProd',
                                         dag=dag
                                         )

            for i in range(cleanup_to_days, cleanup_from_days):

                cleanup_ranks_etcd_prod_ds_minus_i = \
                    DockerBashOperator(task_id='CleanupRanksEtcdProd_DS-%s' % i,
                                       dag=dag,
                                       docker_name='''{{ params.cluster }}''',
                                       bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ macros.ds_add(ds,-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et PRODUCTION -p usage_ranks -pn UsageRanksProd -um failure''' % i
                                       )
                cleanup_prod.set_upstream(cleanup_ranks_etcd_prod_ds_minus_i)

                for target in deploy_targets:
                    cleanup_prod_ds_minus_i = \
                        DockerBashOperator(task_id='Cleanup%s_DS-%s' % (target, i),
                                           dag=dag,
                                           docker_name=target,
                                           bash_command='''{{ params.execution_dir }}/mobile/scripts/windowCleanup.sh -d {{ macros.ds_add(ds,-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -fl APPS -p drop_hbase_tables''' % i
                                           )
                    cleanup_prod_ds_minus_i.set_upstream(copy_to_prod)

                    cleanup_ranks_etcd_prod_ds_minus_i.set_upstream(cleanup_prod_ds_minus_i)

    #########################
    # Dynamic Settings Apps #
    #########################

    update_dynamic_settings_stage = \
        DockerBashOperator(task_id='UpdateDynamicSettingsStage',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et STAGE -p apps_sdk,apps_cross'''
                           )
    update_dynamic_settings_stage.set_upstream(apps)

    if is_prod_env():
        if is_window_dag():
            update_dynamic_settings_prod = \
                DockerBashOperator(task_id='UpdateDynamicSettingsProd',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et PRODUCTION'''
                                   )

            update_dynamic_settings_prod.set_upstream(copy_to_prod)

    #############################
    # Update Availability Dates #
    #############################

    if is_prod_env():
        update_usage_ranks_date_prod = \
            DockerBashOperator(task_id='UpdateUsageRanksDateProd',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et PRODUCTION -p usage_ranks -pn UsageRanksProd -um success'''
                               )
        update_usage_ranks_date_prod.set_upstream(copy_to_prod)

    #################
    # Histograms    #
    #################

    app_sdk_hist_register =  \
        DockerBashOperator(task_id='StoreAppSdkTableSplits',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''source {{ params.execution_dir }}/scripts/common.sh && \
                                           hadoopexec {{ params.execution_dir }}/mobile mobile.jar com.similargroup.common.job.topvalues.KeyHistogramAnalysisUtil \
                                           -in {{ params.base_hdfs_dir }}/{{ params.mode }}/histogram/type={{ params.mode_type }}/{{ macros.generalized_date_partition(ds, params.mode) }}/app-sdk-stats \
                                           -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} \
                                           -k 500000 \
                                           -t app_sdk_stats{{ macros.hbase_table_suffix_partition(ds, params.mode, params.mode_type) }} \
                                           -a
                                        '''
                           )
    app_sdk_hist_register.set_upstream(app_engagement)

    cat_rank_hist_register =  \
        DockerBashOperator(task_id='StoreCategoryRanksTableSplits',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''source {{ params.execution_dir }}/scripts/common.sh && \
                                           hadoopexec {{ params.execution_dir }}/mobile mobile.jar com.similargroup.common.job.topvalues.KeyHistogramAnalysisUtil \
                                           -in {{ params.base_hdfs_dir }}/{{ params.mode }}/histogram/type={{ params.mode_type }}/{{ macros.generalized_date_partition(ds, params.mode) }}/cat-mod-app-rank \
                                           -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} \
                                           -k 50000 \
                                           -t cat_mod_app_rank{{ macros.hbase_table_suffix_partition(ds, params.mode, params.mode_type) }} \
                                           -a
                                        '''
                           )
    cat_rank_hist_register.set_upstream(usage_ranks)

    app_eng_rank_hist_register =  \
        DockerBashOperator(task_id='StoreAppRanksTableSplits',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''source {{ params.execution_dir }}/scripts/common.sh && \
                                           hadoopexec {{ params.execution_dir }}/mobile mobile.jar com.similargroup.common.job.topvalues.KeyHistogramAnalysisUtil \
                                           -in {{ params.base_hdfs_dir }}/{{ params.mode }}/histogram/type={{ params.mode_type }}/{{ macros.generalized_date_partition(ds, params.mode) }}/app-eng-rank \
                                           -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} \
                                           -k 20000 \
                                           -t app_eng_rank{{ macros.hbase_table_suffix_partition(ds, params.mode, params.mode_type) }} \
                                           -a
                                        '''
                           )
    app_eng_rank_hist_register.set_upstream(usage_ranks)

    ###########
    # Wrap-up #
    ###########

    if is_prod_env():

        register_success = KeyValueSetOperator(task_id='RegisterSuccessOnETCD',
                                               dag=dag,
                                               path='''services/mobile_moving_window/{{ params.mode }}/{{ macros.last_interval_day(ds, dag.schedule_interval) }}''',
                                               env='PRODUCTION'
                                               )
        register_success.set_upstream([copy_to_prod, update_usage_ranks_date_prod])


    return dag


globals()['dag_apps_moving_window_snapshot'] = generate_dag(SNAPHOT_MODE)
globals()['dag_apps_moving_window_daily'] = generate_dag(WINDOW_MODE)
