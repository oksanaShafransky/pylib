__author__ = 'Kfir Eittan'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from sw.airflow.key_value import *
from sw.airflow.operators import DockerBashOperator
from sw.airflow.operators import DockerBashSensor
from sw.airflow.operators import  DockerCopyHbaseTableOperator
from sw.airflow.airflow_etcd import EtcdHook
from airflow.operators.python_operator import BranchPythonOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
WINDOW_MODE = 'window'
SNAPHOT_MODE = 'snapshot'
WINDOW_MODE_TYPE = 'last-28'
SNAPSHOT_MODE_TYPE = 'monthly'
DEFAULT_HBASE_CLUSTER = 'hbp1'
IS_PROD = True
DEPLOY_TARGETS = ['hbp1', 'hbp2']

dag_args = {
    'owner': 'similarweb',
    'depends_on_past': False,
    'email': ['kfire@similarweb.com','amitr@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
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

    def mode_dag_name():
        if is_window_dag():
            return 'Window'
        if is_snapshot_dag():
            return 'Snapshot'

    #TODO insert the real logic here
    def is_prod_env():
        return IS_PROD

    dag_args_for_mode = dag_args.copy()
    if is_window_dag():
        dag_args_for_mode.update({'start_date': datetime(2016, 1, 18)})

    if is_snapshot_dag():
        dag_args_for_mode.update({'start_date': datetime(2016, 1, 18)})

    dag_template_params_for_mode = dag_template_params.copy()
    if is_window_dag():
        dag_template_params_for_mode.update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})

    if is_snapshot_dag():
        dag_template_params_for_mode.update({'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

    dag = DAG(dag_id='Desktop_MovingWindow_' + mode_dag_name(), default_args=dag_args_for_mode, params=dag_template_params_for_mode,
              schedule_interval="@daily" if is_window_dag() else "@monthly")

    daily_aggregation = ExternalTaskSensor(external_dag_id='DesktopPreliminary',
                                                   external_task_id='DesktopPreliminary',
                                                   task_id='DailyAggregation',
                                                   dag=dag)

    daily_estimation = ExternalTaskSensor(external_dag_id='DesktopDailyEstimation',
                                                   external_task_id='DesktopDailyEstimation',
                                                   task_id='DailyEstimation',
                                                   dag=dag)

    hbase_tables = \
        DockerBashOperator(task_id='PrepareHBaseTables',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-process.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p tables'''
                           )

    daily_incoming = \
        DockerBashOperator(task_id='DailyIncoming',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p insert_daily_incoming'''
                           )
    daily_incoming.set_upstream(hbase_tables)
    daily_incoming.set_upstream(daily_estimation)

    sum_special_referrer_values = \
        DockerBashOperator(task_id='SumSpecialReferrerValues',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p sum_special_referrer_values'''
                           )
    sum_special_referrer_values.set_upstream(daily_aggregation)

    monthly_sum_estimation_parameters = \
        DockerBashOperator(task_id='MonthlySumEstimationParameters',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p monthly_sum_estimation_parameters'''
                           )
    monthly_sum_estimation_parameters.set_upstream(daily_estimation)

    site_country_special_referrer_distribution = \
        DockerBashOperator(task_id='SiteCountrySpecialReferrerDistribution',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p site_country_special_referrer_distribution'''
                           )
    site_country_special_referrer_distribution.set_upstream(sum_special_referrer_values)
    site_country_special_referrer_distribution.set_upstream(monthly_sum_estimation_parameters)

    traffic_distro = \
        DockerBashOperator(task_id='TrafficDistro',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p traffic_distro,traffic_distro_to_hbase,export_traffic_distro_from_hbase'''
                           )
    traffic_distro.set_upstream(site_country_special_referrer_distribution)

    check_distros = \
        DockerBashOperator(task_id='CheckDistros',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/qa/checkSiteDistro.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p export_traffic_distro_from_hbase'''
                           )
    check_distros.set_upstream(traffic_distro)

    estimate_incoming = \
        DockerBashOperator(task_id='EstimateIncoming',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_incoming'''
                           )
    estimate_incoming.set_upstream(site_country_special_referrer_distribution)

    keywords_prepare = \
        DockerBashOperator(task_id='KeywordsPrepare',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_incoming_keywords,add_totals_to_keys'''
                           )
    keywords_prepare.set_upstream(site_country_special_referrer_distribution)

    ########################
    # the rest map         #
    ########################

    therest_map = \
        DummyOperator(task_id='TherestMap',
                      dag=dag
                      )

    incoming = \
        DockerBashOperator(task_id='Incoming',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p add_totals_to_incoming,also_visited,top_site_to_hbase,prepare_ad_links_incoming,top_site_paid_to_hbase'''
                           )
    incoming.set_upstream(estimate_incoming)
    incoming.set_upstream(hbase_tables)
    therest_map.set_upstream(incoming)

    outgoing = \
        DockerBashOperator(task_id='Outgoing',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_outgoing,add_totals_to_outgoing,prepare_ad_links,top_site,top_site_paid'''
                           )
    outgoing.set_upstream(estimate_incoming)
    outgoing.set_upstream(hbase_tables)
    therest_map.set_upstream(outgoing)

    keywords_paid = \
        DockerBashOperator(task_id='KeywordsPaid',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p sec_paid'''
                           )
    keywords_paid.set_upstream(keywords_prepare)
    keywords_paid.set_upstream(hbase_tables)
    therest_map.set_upstream(keywords_paid)

    keywords_organic = \
        DockerBashOperator(task_id='KeywordsOrganic',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p sec_organic'''
                           )
    keywords_organic.set_upstream(keywords_prepare)
    keywords_organic.set_upstream(hbase_tables)
    therest_map.set_upstream(keywords_organic)

    keywords_top = \
        DockerBashOperator(task_id='KeywordsTop',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p top_value_site_keyword'''
                           )
    keywords_top.set_upstream(keywords_prepare)
    keywords_top.set_upstream(hbase_tables)
    therest_map.set_upstream(keywords_top)

    social_receiving = \
        DockerBashOperator(task_id='SocialReceiving',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/social-receiving.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_social_receiving,add_totals_to_social_receiving,top_site,top_site_paid'''
                           )
    social_receiving.set_upstream(site_country_special_referrer_distribution)
    social_receiving.set_upstream(hbase_tables)
    therest_map.set_upstream(social_receiving)

    sending_pages = \
        DockerBashOperator(task_id='SendingPages',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/sending-pages.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_sending_pages,add_totals_to_sending_pages,prepare_sending_pages_social,top_values_sending_pages_social'''
                           )
    sending_pages.set_upstream(estimate_incoming)
    sending_pages.set_upstream(hbase_tables)
    therest_map.set_upstream(sending_pages)

    info = \
        DockerBashOperator(task_id='Info',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p create_info_table'''
                           )
    info.set_upstream(hbase_tables)

    ranks = \
        DockerBashOperator(task_id='Ranks',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calculate_ranks,export_top_lists,topsites_for_testing'''
                           )
    ranks.set_upstream(monthly_sum_estimation_parameters)
    ranks.set_upstream(info)
    therest_map.set_upstream(ranks)

    misc = \
        DockerBashOperator(task_id='Misc',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/misc.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calculate_subdomains,insert_worldwide_traffic,insert_daily_data'''
                           )
    misc.set_upstream(monthly_sum_estimation_parameters)
    misc.set_upstream(hbase_tables)
    therest_map.set_upstream(misc)

    export_rest = \
        DockerBashOperator(task_id='ExportRest',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p export_rest'''
                           )
    export_rest.set_upstream(therest_map)

    popular_pages = \
        DockerBashOperator(task_id='PopularPages',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/popular-pages.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )
    popular_pages.set_upstream(hbase_tables)
    popular_pages.set_upstream(daily_aggregation)

    if is_snapshot_dag():
        check_snapshot_est = \
            DockerBashOperator(task_id='CheckSnapshotEst',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/qa/SnapshotSiteAndCountryEstimation.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p site_country_special_referrer_distribution'''
                               )
        check_snapshot_est.set_upstream(monthly_sum_estimation_parameters)

        check_customer_snapshot_est = \
            DockerBashOperator(task_id='CheckCustomerSnapshotEst',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/qa/SnapshotCustomerEstimationPerSite.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p site_country_special_referrer_distribution'''
                               )
        check_customer_snapshot_est.set_upstream(monthly_sum_estimation_parameters)

        mobile = \
            DockerBashOperator(task_id='Mobile',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/mobile.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                               )
        mobile.set_upstream(therest_map)
        mobile.set_upstream(export_rest)

        info_lite = \
            DockerBashOperator(task_id='InfoLite',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p create_info_lite'''
                               )
        info_lite.set_upstream(hbase_tables)

        sites_lite = \
            DockerBashOperator(task_id='SitesLite',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/sites-lite.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                               )
        sites_lite.set_upstream(therest_map)

        industry_analysis = \
            DockerBashOperator(task_id='IndustryAnalysis',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/categories.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                               )
        industry_analysis.set_upstream(therest_map)
        industry_analysis.set_upstream(export_rest)

    if is_window_dag():
        check_customers_est = \
            DockerBashOperator(task_id='CheckCustomersEst',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/qa/checkCustomerEstimationPerSite.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                               )
        check_customers_est.set_upstream(daily_estimation)

        check_customer_distros = \
            DockerBashOperator(task_id='CheckCustomerDistros',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/qa/checkCustomerSiteDistro.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                               )
        check_customer_distros.set_upstream(traffic_distro)

    if is_prod_env():

        hbase_suffix_template = ('''{{ params.mode_type }}_{{ macros.ds_format(macros.last_interval_day(ds, dag.schedule_interval), "%Y-%m-%d", "%y_%m_%d")}}''' if is_window_dag() else
                                 '''{{macros.ds_format(macros.last_interval_day(ds, dag.schedule_interval), "%Y-%m-%d", "%y_%m")}}''')

        #######################
        # Copy to Prod        #
        #######################

        copy_to_prod_top_lists = \
            DockerCopyHbaseTableOperator(
                    task_id='CopyToProdTopLists',
                    dag=dag,
                    docker_name='''{{ params.cluster }}''',
                    source_cluster='mrp',
                    target_cluster=','.join(DEPLOY_TARGETS),
                    table_name_template='top_list_' + hbase_suffix_template
            )
        copy_to_prod_top_lists.set_upstream(ranks)

        copy_to_prod_sites_stat = \
            DockerCopyHbaseTableOperator(
                    task_id='CopyToProdSitesStat',
                    dag=dag,
                    docker_name='''{{ params.cluster }}''',
                    source_cluster='mrp',
                    target_cluster=','.join(DEPLOY_TARGETS),
                    table_name_template='sites_stat_' + hbase_suffix_template
            )
        copy_to_prod_sites_stat.set_upstream(therest_map)
        copy_to_prod_sites_stat.set_upstream(popular_pages)
        copy_to_prod_sites_stat.set_upstream(export_rest)
        copy_to_prod_sites_stat.set_upstream(daily_incoming)

        copy_to_prod_sites_info = \
            DockerCopyHbaseTableOperator(
                    task_id='CopyToProdSitesInfo',
                    dag=dag,
                    docker_name='''{{ params.cluster }}''',
                    source_cluster='mrp',
                    target_cluster=','.join(DEPLOY_TARGETS),
                    table_name_template='sites_info_' + hbase_suffix_template
            )
        copy_to_prod_sites_info.set_upstream(therest_map)

        copy_to_prod = DummyOperator(task_id='CopyToProd',
                                     dag=dag)
        copy_to_prod.set_upstream(copy_to_prod_top_lists)
        copy_to_prod.set_upstream(copy_to_prod_sites_stat)
        copy_to_prod.set_upstream(copy_to_prod_sites_info)

        cross_cache_calc = \
            DockerBashOperator(task_id='CrossCacheCalc',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/cross-cache.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p create_hive'''
                               )
        cross_cache_calc.set_upstream(export_rest)

        cross_cache_stage = \
            DockerBashOperator(task_id='CrossCacheStage',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/cross-cache.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et staging -p update_bucket'''
                               )
        cross_cache_stage.set_upstream(cross_cache_calc)

        cross_cache_prod = \
            DockerBashOperator(task_id='CrossCacheProd',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/cross-cache.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et production -p update_bucket'''
                               )
        cross_cache_prod.set_upstream(cross_cache_calc)

        for target in DEPLOY_TARGETS:
            dynamic_cross_prod_per_target = \
                DockerBashOperator(task_id='DynamicCrossProd_%s' % target,
                                   dag=dag,
                                   docker_name='''{{ params.cluster-%s }}''' % target,
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et production -p update_cross_cache'''
                                   )
            dynamic_cross_prod_per_target.set_upstream(cross_cache_prod)

        dynamic_stage = \
            DockerBashOperator(task_id='DynamicStage',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et staging -p update_pro,update_special_referrers_stage'''
                               )
        dynamic_stage.set_upstream(export_rest)
        dynamic_stage.set_upstream(popular_pages)
        dynamic_stage.set_upstream(daily_incoming)

        dynamic_cross_stage = \
            DockerBashOperator(task_id='DynamicCacheStage',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et staging -p update_cross_cache'''
                               )
        dynamic_cross_stage.set_upstream(cross_cache_stage)
        dynamic_cross_stage.set_upstream(dynamic_stage)

        dynamic_prod = DummyOperator(task_id='DynamicProd',
                                     dag=dag)

        for target in DEPLOY_TARGETS:
            dynamic_prod_per_target = \
                DockerBashOperator(task_id='DynamicProd_%s' % target,
                                   dag=dag,
                                   docker_name='''{{ params.cluster-%s }}''' % target,
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et production -p update_pro,update_special_referrers_prod'''
                                   )
            dynamic_prod_per_target.set_upstream(copy_to_prod)
            dynamic_prod.set_upstream(dynamic_prod_per_target)

        repair_incoming_tables = \
            DockerBashOperator(task_id='RepairIncomingTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_incoming_tables.set_upstream(incoming)

        repair_outgoing_tables = \
            DockerBashOperator(task_id='RepairOutgoingTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_outgoing_tables.set_upstream(outgoing)

        repair_keywords_tables = \
            DockerBashOperator(task_id='RepairKeywordsTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_keywords_tables.set_upstream(keywords_paid)
        repair_keywords_tables.set_upstream(keywords_organic)
        repair_keywords_tables.set_upstream(keywords_top)

        repair_ranks_tables = \
            DockerBashOperator(task_id='RepairRanksTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_ranks_tables.set_upstream(ranks)
        repair_ranks_tables.set_upstream(export_rest)

        repair_sr_tables = \
            DockerBashOperator(task_id='RepairSrTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_sr_tables.set_upstream(traffic_distro)

        repair_sending_pages_tables = \
            DockerBashOperator(task_id='RepairSendingPagesTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/sending-pages.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_sending_pages_tables.set_upstream(sending_pages)

        repair_popular_pages_tables = \
            DockerBashOperator(task_id='RepairPopularPagesTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/popular-pages.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_popular_pages_tables.set_upstream(popular_pages)

        repair_social_receiving_tables = \
            DockerBashOperator(task_id='RepairSocialReceivingTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/social-receiving.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_social_receiving_tables.set_upstream(social_receiving)

        if is_snapshot_dag():

            copy_to_prod_mobile_keyword_apps = \
                DockerCopyHbaseTableOperator(
                        task_id='CopyToProdMobileKeywordApps',
                        dag=dag,
                        docker_name='''{{ params.cluster }}''',
                        source_cluster='mrp',
                        target_cluster=','.join(DEPLOY_TARGETS),
                        table_name_template='mobile_keyword_apps_' + hbase_suffix_template
                )
            copy_to_prod_mobile_keyword_apps.set_upstream(mobile)

            copy_to_prod_app_stat = \
                DockerCopyHbaseTableOperator(
                        task_id='CopyToProdMobileAppStat',
                        dag=dag,
                        docker_name='''{{ params.cluster }}''',
                        source_cluster='mrp',
                        target_cluster=','.join(DEPLOY_TARGETS),
                        table_name_template='app_stat_' + hbase_suffix_template
                )
            copy_to_prod_app_stat.set_upstream(mobile)

            copy_to_prod_top_app_keywords = \
                DockerCopyHbaseTableOperator(
                        task_id='CopyToProdMobileAppKeywords',
                        dag=dag,
                        docker_name='''{{ params.cluster }}''',
                        source_cluster='mrp',
                        target_cluster=','.join(DEPLOY_TARGETS),
                        table_name_template='top_app_keywords_' + hbase_suffix_template
                )
            copy_to_prod_top_app_keywords.set_upstream(mobile)

            copy_to_prod_mobile = DummyOperator(task_id='CopyToProdMobile',
                                                dag=dag)
            copy_to_prod_mobile.set_upstream(copy_to_prod_mobile_keyword_apps)
            copy_to_prod_mobile.set_upstream(copy_to_prod_top_app_keywords)
            copy_to_prod_mobile.set_upstream(copy_to_prod_app_stat)
            copy_to_prod.set_upstream(copy_to_prod_mobile)

            copy_to_prod_snapshot_industry = \
                DockerCopyHbaseTableOperator(
                        task_id='CopyToProdSnapshotIndustry',
                        dag=dag,
                        docker_name='''{{ params.cluster }}''',
                        source_cluster='mrp',
                        target_cluster=','.join(DEPLOY_TARGETS),
                        table_name_template='categories_' + hbase_suffix_template
                )
            copy_to_prod_snapshot_industry.set_upstream(ranks)

            copy_to_prod_snapshot_sites_scrape_stat = \
                DockerCopyHbaseTableOperator(
                        task_id='CopyToProdSnapshotSitesScrapeStat',
                        dag=dag,
                        docker_name='''{{ params.cluster }}''',
                        source_cluster='mrp',
                        target_cluster=','.join(DEPLOY_TARGETS),
                        table_name_template='sites_scrape_stat_' + hbase_suffix_template
                )
            copy_to_prod_snapshot_sites_scrape_stat.set_upstream(therest_map)

            copy_to_prod_snapshot_sites_lite = \
                DockerCopyHbaseTableOperator(
                        task_id='CopyToProdSnapshotSitesLite',
                        dag=dag,
                        docker_name='''{{ params.cluster }}''',
                        source_cluster='mrp',
                        target_cluster=','.join(DEPLOY_TARGETS),
                        table_name_template='sites_lite_' + hbase_suffix_template
                )
            copy_to_prod_snapshot_sites_lite.set_upstream(therest_map)

            copy_to_prod_snapshot = DummyOperator(task_id='CopyToProdSnapshot',
                                                  dag=dag)
            copy_to_prod_snapshot.set_upstream(copy_to_prod_snapshot_industry)
            copy_to_prod_snapshot.set_upstream(copy_to_prod_snapshot_sites_scrape_stat)
            copy_to_prod_snapshot.set_upstream(copy_to_prod_snapshot_sites_lite)
            copy_to_prod.set_upstream(copy_to_prod_snapshot)

            dynamic_stage_lite = \
                DockerBashOperator(task_id='DynamicStageLite',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et staging -p update_lite'''
                                   )
            dynamic_stage_lite.set_upstream(sites_lite)

            dynamic_stage_industry = \
                DockerBashOperator(task_id='DynamicStageIndustry',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et staging -p update_categories'''
                                   )
            dynamic_stage_industry.set_upstream(industry_analysis)

            dynamic_prod_lite = DummyOperator(task_id='DynamicProdLite',
                                              dag=dag)
            for target in DEPLOY_TARGETS:
                dynamic_prod_lite_per_target = \
                    DockerBashOperator(task_id='DynamicProdLite_%s' % target,
                                       dag=dag,
                                       docker_name='''{{ params.cluster-%s }}''' % target,
                                       bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et production -p update_lite'''
                                       )
                dynamic_prod_lite_per_target.set_upstream(copy_to_prod_snapshot)
                dynamic_prod_lite_per_target.set_upstream(sites_lite)
                dynamic_prod_lite.set_upstream(dynamic_prod_lite_per_target)

            dynamic_prod_industry = DummyOperator(task_id='DynamicProdIndustry',
                                                  dag=dag)
            for target in DEPLOY_TARGETS:
                dynamic_prod_industry_per_target = \
                    DockerBashOperator(task_id='DynamicProdIndustry_%s' % target,
                                       dag=dag,
                                       docker_name='''{{ params.cluster-%s }}''' % target,
                                       bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et production -p update_categories'''
                                       )
                dynamic_prod_industry_per_target.set_upstream(copy_to_prod_snapshot)
                dynamic_prod_industry_per_target.set_upstream(industry_analysis)
                dynamic_prod_industry.set_upstream(dynamic_prod_industry_per_target)

            repair_mobile = \
                DockerBashOperator(task_id='RepairMobile',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/mobile.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                                   )
            repair_mobile.set_upstream(mobile)

            sitemap = \
                DockerBashOperator(task_id='Sitemap',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/sitemap.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                                   )
            sitemap.set_upstream(dynamic_prod_lite)
            sitemap.set_upstream(dynamic_prod_industry)

            web_autocomplete = \
                DockerBashOperator(task_id='WebAutocomplete',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/web_autocomplete.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                                   )
            web_autocomplete.set_upstream(cross_cache_prod)


        if is_window_dag():

            cleanup_from_days = 8
            cleanup_to_days = 3

            cleanup_stage = DummyOperator(task_id='CleanupStage',
                                          dag=dag)
            for i in range(cleanup_to_days, cleanup_from_days):
                if i == cleanup_to_days:
                    cleanup_stage_stages = 'drop_crosscache_stage'
                else:
                    cleanup_stage_stages = 'drop_crosscache_stage,delete_files,drop_hbase_tables'

                cleanup_stage_ds_minus_i = \
                    DockerBashOperator(task_id='CleanupStage_DS-%s' % i,
                                       dag=dag,
                                       docker_name='''{{ params.cluster }}''',
                                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/windowCleanup.sh -d {{ macros.ds_add(macros.last_interval_day(ds, dag.schedule_interval),-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et staging -p %s''' % (i, cleanup_stage_stages)
                                       )
                cleanup_stage_ds_minus_i.set_upstream(cross_cache_stage)
                cleanup_stage_ds_minus_i.set_upstream(dynamic_stage)
                cleanup_stage.set_upstream(cleanup_stage_ds_minus_i)

            cleanup_from_days = 12
            cleanup_to_days = 4

            cleanup_prod = DummyOperator(task_id='CleanupProd',
                                         dag=dag)
            for i in range(cleanup_to_days, cleanup_from_days):
                if i == cleanup_to_days:
                    cleanup_prod_stages = 'drop_crosscache_prod'
                else:
                    cleanup_prod_stages = 'drop_crosscache_prod,drop_hbase_tables'

                cleanup_prod_ds_minus_i = DummyOperator(task_id='CleanupProd_DS-%s' % i,
                                             dag=dag)

                for target in DEPLOY_TARGETS:
                    cleanup_prod_per_target_ds_minus_i = \
                        DockerBashOperator(task_id='CleanupProd_%s_DS-%s' % (target, i),
                                           dag=dag,
                                           docker_name='''{{ params.cluster-%s }}''' % target,
                                           bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/windowCleanup.sh -d {{ macros.ds_add(macros.last_interval_day(ds, dag.schedule_interval),-%s) }}  -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et production -p %s''' % (i, cleanup_prod_stages)
                                           )
                    cleanup_prod_per_target_ds_minus_i.set_upstream(cross_cache_prod)
                    cleanup_prod_per_target_ds_minus_i.set_upstream(dynamic_prod)
                    cleanup_prod_ds_minus_i.set_upstream(cleanup_prod_per_target_ds_minus_i)

                cleanup_prod.set_upstream(cleanup_prod_ds_minus_i)

        ###########
        # Wrap-up #
        ###########

        register_success = KeyValueSetOperator(task_id='RegisterSuccessOnETCD',
                                               dag=dag,
                                               path='''services/bigdata_orchestration/{{ params.mode }}/{{ macros.last_interval_day(ds, dag.schedule_interval) }}''',
                                               env='PRODUCTION'
                                               )
        register_success.set_upstream(copy_to_prod)


    return dag


globals()['dag_desktop_moving_window_snapshot'] = generate_dags(SNAPHOT_MODE)
globals()['dag_desktop_moving_window_daily'] = generate_dags(WINDOW_MODE)
