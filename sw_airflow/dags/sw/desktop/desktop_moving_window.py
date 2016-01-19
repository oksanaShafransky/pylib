__author__ = 'Kfir Eittan'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.sensors import HdfsSensor
from sw.airflow.key_value import *
from sw.airflow.operators import DockerBashOperator
from sw.airflow.operators import DockerBashSensor
from sw.airflow.operators import DockerCopyHbaseTableOperator
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
        dag_args_for_mode.update({'start_date': datetime(2016, 1, 17)})

    if is_snapshot_dag():
        dag_args_for_mode.update({'start_date': datetime(2016, 1, 17)})

    dag_template_params_for_mode = dag_template_params.copy()
    if is_window_dag():
        dag_template_params_for_mode.update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})

    if is_snapshot_dag():
        dag_template_params_for_mode.update({'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

    dag = DAG(dag_id='Desktop_MovingWindow_' + mode_dag_name(), default_args=dag_args_for_mode, params=dag_template_params_for_mode,
              schedule_interval="@daily" if is_window_dag() else "@monthly")

    hbase_tables = \
        DockerBashOperator(task_id='HBaseTables',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p tables'''
                           )

    daily_aggregation = ExternalTaskSensor(external_dag_id='DesktopPreliminary',
                                                   external_task_id='DesktopPreliminary',
                                                   task_id='DailyAggregation',
                                                   dag=dag)
    daily_aggregation.set_upstream(hbase_tables)

    daily_estimation = ExternalTaskSensor(external_dag_id='DesktopDailyEstimation',
                                                   external_task_id='DesktopDailyEstimation',
                                                   task_id='DailyEstimation',
                                                   dag=dag)
    daily_estimation.set_upstream(hbase_tables)

    info = \
        DockerBashOperator(task_id='Info',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p create_info_table'''
                           )
    info.set_upstream(hbase_tables)

    daily_incoming = \
        DockerBashOperator(task_id='DailyIncoming',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p insert_daily_incoming'''
                           )
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
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p traffic_distro'''
                           )
    traffic_distro.set_upstream(site_country_special_referrer_distribution)

    traffic_distro_to_hbase = \
        DockerBashOperator(task_id='TrafficDistroToHBase',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p traffic_distro_to_hbase'''
                           )
    traffic_distro_to_hbase.set_upstream(traffic_distro)

    traffic_distro_export_from_hbase = \
        DockerBashOperator(task_id='TrafficDistroExportFromHBase',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p export_traffic_distro_from_hbase'''
                           )
    traffic_distro_export_from_hbase.set_upstream(traffic_distro_to_hbase)

    incoming_estimate = \
        DockerBashOperator(task_id='IncomingEstimation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_incoming'''
                           )
    incoming_estimate.set_upstream(site_country_special_referrer_distribution)

    keywords_estimation = \
        DockerBashOperator(task_id='KeywordsEstimation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_incoming_keywords'''
                           )
    keywords_estimation.set_upstream(site_country_special_referrer_distribution)

    keywords_add_totals = \
        DockerBashOperator(task_id='KeywordsAddTotals',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p add_totals_to_keys'''
                           )
    keywords_add_totals.set_upstream(keywords_estimation)

    incoming_add_totals = \
        DockerBashOperator(task_id='IncomingAddTotals',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p add_totals_to_incoming'''
                           )
    incoming_add_totals.set_upstream(incoming_estimate)

    also_visited = \
        DockerBashOperator(task_id='AlsoVisited',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p also_visited'''
                           )
    also_visited.set_upstream(incoming_add_totals)

    incoming_to_hbase = \
        DockerBashOperator(task_id='IncomingToHBase',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p top_site_to_hbase'''
                           )
    incoming_to_hbase.set_upstream(incoming_add_totals)

    incoming_paid = \
        DockerBashOperator(task_id='IncomingPaid',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p prepare_ad_links_incoming'''
                           )
    incoming_paid.set_upstream(incoming_add_totals)

    incoming_paid_to_hbase = \
        DockerBashOperator(task_id='IncomingPaidToHBase',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p top_site_paid_to_hbase'''
                           )
    incoming_paid_to_hbase.set_upstream(incoming_paid)

    incoming = \
        DummyOperator(task_id='Incoming',
                      dag=dag
                      )
    incoming.set_upstream(also_visited)
    incoming.set_upstream(incoming_to_hbase)
    incoming.set_upstream(incoming_paid_to_hbase)

    outgoing_estimation = \
        DockerBashOperator(task_id='OutgoingEstimation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_outgoing'''
                           )
    outgoing_estimation.set_upstream(incoming_estimate)

    outgoing_add_totals = \
        DockerBashOperator(task_id='OutgoingAddTotals',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p add_totals_to_outgoing'''
                           )
    outgoing_add_totals.set_upstream(outgoing_estimation)

    outgoing_to_hbase = \
        DockerBashOperator(task_id='OutgoingToHBase',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p top_site'''
                           )
    outgoing_to_hbase.set_upstream(outgoing_add_totals)

    outgoing_paid = \
        DockerBashOperator(task_id='OutgoingPaid',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p prepare_ad_links'''
                           )
    outgoing_paid.set_upstream(outgoing_add_totals)

    outgoing_paid_to_hbase = \
        DockerBashOperator(task_id='OutgoingPaidToHBase',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p top_site_paid'''
                           )
    outgoing_paid_to_hbase.set_upstream(outgoing_paid)

    outgoing = \
        DummyOperator(task_id='Outgoing',
                      dag=dag
                      )
    outgoing.set_upstream(outgoing_to_hbase)
    outgoing.set_upstream(outgoing_paid_to_hbase)

    keywords_paid = \
        DockerBashOperator(task_id='KeywordsPaid',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p sec_paid'''
                           )
    keywords_paid.set_upstream(keywords_add_totals)

    keywords_organic = \
        DockerBashOperator(task_id='KeywordsOrganic',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p sec_organic'''
                           )
    keywords_organic.set_upstream(keywords_add_totals)

    keywords_top = \
        DockerBashOperator(task_id='KeywordsTop',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p top_value_site_keyword'''
                           )
    keywords_top.set_upstream(keywords_add_totals)

    keywords = \
        DummyOperator(task_id='Keywords',
                      dag=dag
                      )
    keywords.set_upstream(keywords_paid)
    keywords.set_upstream(keywords_organic)
    keywords.set_upstream(keywords_top)

    social_receiving = \
        DockerBashOperator(task_id='SocialReceiving',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/social-receiving.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_social_receiving,add_totals_to_social_receiving,top_site,top_site_paid'''
                           )
    social_receiving.set_upstream(site_country_special_referrer_distribution)

    sending_pages = \
        DockerBashOperator(task_id='SendingPages',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/sending-pages.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_sending_pages,add_totals_to_sending_pages,prepare_sending_pages_social,top_values_sending_pages_social'''
                           )
    sending_pages.set_upstream(incoming_estimate)

    ranks = \
        DockerBashOperator(task_id='Ranks',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calculate_ranks,export_top_lists,topsites_for_testing'''
                           )
    ranks.set_upstream(monthly_sum_estimation_parameters)
    ranks.set_upstream(info)

    misc = \
        DockerBashOperator(task_id='Misc',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/misc.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calculate_subdomains,insert_worldwide_traffic,insert_daily_data'''
                           )
    misc.set_upstream(monthly_sum_estimation_parameters)

    conditionals = \
        DummyOperator(task_id='Conditionals',
                      dag=dag
                      )
    conditionals.set_upstream(incoming)
    conditionals.set_upstream(outgoing)
    conditionals.set_upstream(keywords)
    conditionals.set_upstream(social_receiving)
    conditionals.set_upstream(sending_pages)
    conditionals.set_upstream(ranks)
    conditionals.set_upstream(misc)
    conditionals.set_upstream(traffic_distro_export_from_hbase)

    export_rest = \
        DockerBashOperator(task_id='ExportRest',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p export_rest'''
                           )
    export_rest.set_upstream(misc)
    export_rest.set_upstream(ranks)

    popular_pages = \
        DockerBashOperator(task_id='PopularPages',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/popular-pages.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )
    popular_pages.set_upstream(daily_aggregation)

    if is_snapshot_dag():
        info_lite = \
            DockerBashOperator(task_id='InfoLite',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p create_info_lite'''
                               )
        info_lite.set_upstream(hbase_tables)

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
        mobile.set_upstream(conditionals)
        mobile.set_upstream(export_rest)

        sites_lite = \
            DockerBashOperator(task_id='SitesLite',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/sites-lite.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                               )
        sites_lite.set_upstream(conditionals)

        industry_analysis = \
            DockerBashOperator(task_id='IndustryAnalysis',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/categories.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                               )
        industry_analysis.set_upstream(export_rest)

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
        copy_to_prod_sites_stat.set_upstream(popular_pages)
        copy_to_prod_sites_stat.set_upstream(export_rest)
        copy_to_prod_sites_stat.set_upstream(daily_incoming)
        copy_to_prod_sites_stat.set_upstream(conditionals)

        copy_to_prod_sites_info = \
            DockerCopyHbaseTableOperator(
                    task_id='CopyToProdSitesInfo',
                    dag=dag,
                    docker_name='''{{ params.cluster }}''',
                    source_cluster='mrp',
                    target_cluster=','.join(DEPLOY_TARGETS),
                    table_name_template='sites_info_' + hbase_suffix_template
            )
        copy_to_prod_sites_info.set_upstream(export_rest)

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

        dynamic_prod = DummyOperator(task_id='DynamicProd',
                             dag=dag)
        dynamic_cross_prod = DummyOperator(task_id='DynamicCrossProd',
                                     dag=dag)
        for target in DEPLOY_TARGETS:
            dynamic_cross_prod_per_target = \
                DockerBashOperator(task_id='DynamicCrossProd_%s' % target,
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}-%s''' % target,
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et production -p update_cross_cache'''
                                   )
            dynamic_cross_prod_per_target.set_upstream(dynamic_prod)
            dynamic_cross_prod_per_target.set_upstream(cross_cache_prod)
            dynamic_cross_prod.set_upstream(dynamic_cross_prod_per_target)

        dynamic_stage = \
            DockerBashOperator(task_id='DynamicStage',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et staging -p update_pro,update_special_referrers_stage'''
                               )
        dynamic_stage.set_upstream(export_rest)
        dynamic_stage.set_upstream(popular_pages)
        dynamic_stage.set_upstream(daily_incoming)
        dynamic_stage.set_upstream(conditionals)

        dynamic_cross_stage = \
            DockerBashOperator(task_id='DynamicCacheStage',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et staging -p update_cross_cache'''
                               )
        dynamic_cross_stage.set_upstream(cross_cache_stage)
        dynamic_cross_stage.set_upstream(dynamic_stage)


        for target in DEPLOY_TARGETS:
            dynamic_prod_per_target = \
                DockerBashOperator(task_id='DynamicProd_%s' % target,
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}-%s''' % target,
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et production -p update_pro,update_special_referrers_prod'''
                                   )
            dynamic_prod_per_target.set_upstream(copy_to_prod)
            dynamic_prod.set_upstream(dynamic_prod_per_target)

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
            copy_to_prod_snapshot_sites_scrape_stat.set_upstream(conditionals)

            copy_to_prod_snapshot_sites_lite = \
                DockerCopyHbaseTableOperator(
                        task_id='CopyToProdSnapshotSitesLite',
                        dag=dag,
                        docker_name='''{{ params.cluster }}''',
                        source_cluster='mrp',
                        target_cluster=','.join(DEPLOY_TARGETS),
                        table_name_template='sites_lite_' + hbase_suffix_template
                )
            copy_to_prod_snapshot_sites_lite.set_upstream(conditionals)

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
                                       docker_name='''{{ params.cluster }}-%s''' % target,
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
                                       docker_name='''{{ params.cluster }}-%s''' % target,
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

        ###########
        # Wrap-up #
        ###########

        register_success = KeyValueSetOperator(task_id='RegisterSuccessOnETCD',
                                               dag=dag,
                                               path='''services/bigdata_orchestration/{{ params.mode }}/{{ macros.last_interval_day(ds, dag.schedule_interval) }}''',
                                               env='PRODUCTION'
                                               )
        register_success.set_upstream(dynamic_prod)
        if is_snapshot_dag():
            register_success.set_upstream(sitemap)
            register_success.set_upstream(web_autocomplete)
            register_success.set_upstream(dynamic_cross_prod)

        moving_window = \
            DummyOperator(task_id='MovingWindow_%s' % mode_dag_name(),
                          dag=dag
                          )
        moving_window.set_upstream(register_success)

        ##################
        # Non Operationals #
        ##################


        non_operationals = \
            DummyOperator(task_id='NonOperationals',
                          dag=dag
                          )
        non_operationals.set_upstream(register_success)

        repair_tables = \
            DummyOperator(task_id='RepairTables',
                          dag=dag
                          )
        repair_tables.set_upstream(non_operationals)

        repair_incoming_tables = \
            DockerBashOperator(task_id='RepairIncomingTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_incoming_tables.set_upstream(repair_tables)

        repair_outgoing_tables = \
            DockerBashOperator(task_id='RepairOutgoingTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_outgoing_tables.set_upstream(repair_tables)

        repair_keywords_tables = \
            DockerBashOperator(task_id='RepairKeywordsTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_keywords_tables.set_upstream(repair_tables)

        repair_ranks_tables = \
            DockerBashOperator(task_id='RepairRanksTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_ranks_tables.set_upstream(repair_tables)

        repair_sr_tables = \
            DockerBashOperator(task_id='RepairSrTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_sr_tables.set_upstream(repair_tables)

        repair_sending_pages_tables = \
            DockerBashOperator(task_id='RepairSendingPagesTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/sending-pages.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_sending_pages_tables.set_upstream(repair_tables)

        repair_popular_pages_tables = \
            DockerBashOperator(task_id='RepairPopularPagesTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/popular-pages.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_popular_pages_tables.set_upstream(repair_tables)

        repair_social_receiving_tables = \
            DockerBashOperator(task_id='RepairSocialReceivingTables',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/social-receiving.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p repair'''
                               )
        repair_social_receiving_tables.set_upstream(repair_tables)

        check_distros = \
            DockerBashOperator(task_id='CheckDistros',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/qa/checkSiteDistro.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                               )
        check_distros.set_upstream(non_operationals)

        if is_window_dag():
            check_customers_est = \
                DockerBashOperator(task_id='CheckCustomersEst',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/qa/checkCustomerEstimationPerSite.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                                   )
            check_customers_est.set_upstream(non_operationals)

            check_customer_distros = \
                DockerBashOperator(task_id='CheckCustomerDistros',
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/qa/checkCustomerSiteDistro.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                                   )
            check_customer_distros.set_upstream(non_operationals)

            cleanup_from_days = 8
            cleanup_to_days = 4

            cleanup_stage = DummyOperator(task_id='CleanupStage',
                                          dag=dag)

            for i in range(cleanup_to_days, cleanup_from_days):
                cleanup_stage_ds_minus_i = \
                    DockerBashOperator(task_id='CleanupStage_DS-%s' % i,
                                       dag=dag,
                                       docker_name='''{{ params.cluster }}''',
                                       bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/windowCleanup.sh -d {{ macros.ds_add(macros.last_interval_day(ds, dag.schedule_interval),-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et staging -p delete_files,drop_hbase_tables''' % i
                                       )
                cleanup_stage_ds_minus_i.set_upstream(cleanup_stage)

            non_operationals.set_upstream(dynamic_cross_stage)
            cleanup_stage.set_upstream(non_operationals)

            cleanup_from_days = 12
            cleanup_to_days = 5

            cleanup_prod = DummyOperator(task_id='CleanupProd',
                                         dag=dag)

            for i in range(cleanup_to_days, cleanup_from_days):
                cleanup_prod_ds_minus_i = DummyOperator(task_id='CleanupProd_DS-%s' % i,
                                                        dag=dag)
                cleanup_prod_ds_minus_i.set_upstream(cleanup_prod)

                for target in DEPLOY_TARGETS:
                    cleanup_prod_per_target_ds_minus_i = \
                        DockerBashOperator(task_id='CleanupProd_%s_DS-%s' % (target, i),
                                           dag=dag,
                                           docker_name='%s' % target,
                                           bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/windowCleanup.sh -d {{ macros.ds_add(macros.last_interval_day(ds, dag.schedule_interval),-%s) }}  -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et production -p drop_hbase_tables''' % i
                                           )
                    cleanup_prod_per_target_ds_minus_i.set_upstream(cleanup_prod_ds_minus_i)

            non_operationals.set_upstream(dynamic_cross_prod)
            cleanup_prod.set_upstream(non_operationals)

    #################
    # Histograms    #
    #################

    sites_stat_hist_sensor = \
        HdfsSensor(task_id='SiteStatsHistogramReady',
                   dag=dag,
                   hdfs_conn_id='hdfs_%s' % DEFAULT_CLUSTER,
                   filepath='''{{ params.base_hdfs_dir }}/{{ params.mode }}/histogram/type={{ params.mode_type }}/{{ macros.date_partition(ds) }}/sites-stat/_SUCCESS''',
                   execution_timeout=timedelta(minutes=600)
                   )

    sites_stat_hist_register =  \
        DockerBashOperator(task_id='StoreSiteStatsTableSplits',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''source {{ params.execution_dir }}/scripts/common.sh && \
                                           hadoopexec {{ params.execution_dir }} mobile.jar com.similargroup.common.job.topvalues.KeyHistogramAnalysisUtil \
                                           -in {{ params.base_hdfs_dir }}/{{ params.mode }}/histogram/type={{ params.mode_type }}/{{ macros.date_partition(ds) }}/sites-stat \
                                           -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} \
                                           -k 2000000 \
                                           -t app_sdk_stats{{ macros.hbase_table_suffix_partition(ds, params.mode, params.mode_type) }}
                                        '''
                           )
    sites_stat_hist_register.set_upstream(sites_stat_hist_sensor)

    site_info_hist_sensor = \
        HdfsSensor(task_id='SiteInfoHistogramReady',
                   dag=dag,
                   hdfs_conn_id='hdfs_%s' % DEFAULT_CLUSTER,
                   filepath='''{{ params.base_hdfs_dir }}/{{ params.mode }}/histogram/type={{ params.mode_type }}/{{ macros.date_partition(ds) }}/sites-info/_SUCCESS''',
                   execution_timeout=timedelta(minutes=600)
                   )

    site_info_hist_register =  \
        DockerBashOperator(task_id='StoreSiteInfoTableSplits',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''source {{ params.execution_dir }}/scripts/common.sh && \
                                           hadoopexec {{ params.execution_dir }} mobile.jar com.similargroup.common.job.topvalues.KeyHistogramAnalysisUtil \
                                           -in {{ params.base_hdfs_dir }}/{{ params.mode }}/histogram/type={{ params.mode_type }}/{{ macros.date_partition(ds) }}/sites-info \
                                           -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} \
                                           -k 2000000 \
                                           -t app_sdk_stats{{ macros.hbase_table_suffix_partition(ds, params.mode, params.mode_type) }}
                                        '''
                           )
    site_info_hist_register.set_upstream(site_info_hist_sensor)


    return dag


globals()['dag_desktop_moving_window_snapshot'] = generate_dags(SNAPHOT_MODE)
globals()['dag_desktop_moving_window_daily'] = generate_dags(WINDOW_MODE)
