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

    #TODO insert the real logic here
    def is_prod_env():
        return True

    dag_args_for_mode = dag_args.copy()
    if is_window_dag():
        dag_args_for_mode.update({'start_date': datetime(2016, 2, 1)})

    if is_snapshot_dag():
        dag_args_for_mode.update({'start_date': datetime(2016, 2, 1)})

    dag_template_params_for_mode = dag_template_params.copy()
    if is_window_dag():
        dag_template_params_for_mode.update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})

    if is_snapshot_dag():
        dag_template_params_for_mode.update({'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

    dag = DAG(dag_id='Desktop_MovingWindow_' + mode, default_args=dag_args_for_mode, params=dag_template_params_for_mode,
              schedule_interval=timedelta(days=1))

    desktop_daily_aggregation = ExternalTaskSensor(external_dag_id='DesktopPreliminary',
                                                   external_task_id='DesktopPreliminary',
                                                   task_id='DailyAggregation',
                                                   dag=dag)

    desktop_daily_estimation = ExternalTaskSensor(external_dag_id='DesktopDailyEstimation',
                                                   external_task_id='DesktopDailyEstimation',
                                                   task_id='DailyEstimation',
                                                   dag=dag)

    ########################
    # Prepare HBase Tables #
    ########################

    hbase_tables = \
        DockerBashOperator(task_id='PrepareHBaseTables',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-process.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p tables'''
                           )

    hbase_tables.set_upstream(desktop_daily_aggregation)

    ########################
    # Insert Daily Incoming#
    ########################

    insert_daily_incoming = \
        DockerBashOperator(task_id='InsertDailyIncoming',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p insert_daily_incoming'''
                           )

    insert_daily_incoming.set_upstream(hbase_tables)
    insert_daily_incoming.set_upstream(desktop_daily_estimation)

    ########################
    # Hierarhical bootsrap #
    ########################

    sum_special_referrer_values = \
        DockerBashOperator(task_id='SumSpecialReferrerValues',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p sum_special_referrer_values'''
                           )

    sum_special_referrer_values.set_upstream(desktop_daily_aggregation)

    monthly_sum_estimation_parameters = \
        DockerBashOperator(task_id='MonthlySumEstimationParameters',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p monthly_sum_estimation_parameters'''
                           )

    monthly_sum_estimation_parameters.set_upstream(desktop_daily_estimation)

    site_country_special_referrer_distribution = \
        DockerBashOperator(task_id='SiteCountrySpecialReferrerDistribution',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p site_country_special_referrer_distribution'''
                           )

    site_country_special_referrer_distribution.set_upstream(sum_special_referrer_values)
    site_country_special_referrer_distribution.set_upstream(monthly_sum_estimation_parameters)

    check_snapshot_est = \
        DockerBashOperator(task_id='CheckSnapshotEst',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/qa/SnapshotSiteAndCountryEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p site_country_special_referrer_distribution'''
                           )

    check_snapshot_est.set_upstream(monthly_sum_estimation_parameters)

    check_customer_snapshot_est = \
        DockerBashOperator(task_id='CheckCustomerSnapshotEst',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/qa/SnapshotCustomerEstimationPerSite.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p site_country_special_referrer_distribution'''
                           )

    check_customer_snapshot_est.set_upstream(monthly_sum_estimation_parameters)

    ########################
    # Traffic Distro       #
    ########################

    traffic_distro = \
        DockerBashOperator(task_id='TrafficDistro',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p traffic_distro,traffic_distro_to_hbase,export_traffic_distro_from_hbase'''
                           )

    traffic_distro.set_upstream(site_country_special_referrer_distribution)

    check_distros = \
        DockerBashOperator(task_id='CheckDistros',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/qa/checkSiteDistro.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p export_traffic_distro_from_hbase'''
                           )

    check_distros.set_upstream(traffic_distro)

    ########################
    # Estimate Incoming    #
    ########################

    estimate_incoming = \
        DockerBashOperator(task_id='EstimateIncoming',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_incoming'''
                           )

    estimate_incoming.set_upstream(site_country_special_referrer_distribution)

    ########################
    # the rest map         #
    ########################

    incoming = \
        DockerBashOperator(task_id='Incoming',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p add_totals_to_incoming,also_visited,top_site_to_hbase,prepare_ad_links_incoming,top_site_paid_to_hbase'''
                           )

    incoming.set_upstream(estimate_incoming)
    incoming.set_upstream(hbase_tables)

    outgoing = \
        DockerBashOperator(task_id='Outgoing',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_outgoing,add_totals_to_outgoing,prepare_ad_links,top_site,top_site_paid'''
                           )

    outgoing.set_upstream(estimate_incoming)
    outgoing.set_upstream(hbase_tables)

    keywords_prepare = \
        DockerBashOperator(task_id='KeywordsPrepare',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_incoming_keywords,add_totals_to_keys'''
                           )

    keywords_prepare.set_upstream(site_country_special_referrer_distribution)

    keywords_paid = \
        DockerBashOperator(task_id='KeywordsPaid',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p sec_paid'''
                           )

    keywords_paid.set_upstream(keywords_prepare)
    keywords_paid.set_upstream(hbase_tables)

    keywords_organic = \
        DockerBashOperator(task_id='KeywordsOrganic',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p sec_organic'''
                           )

    keywords_organic.set_upstream(keywords_prepare)
    keywords_organic.set_upstream(hbase_tables)

    keywords_top = \
        DockerBashOperator(task_id='KeywordsTop',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p top_value_site_keyword'''
                           )

    keywords_top.set_upstream(keywords_prepare)
    keywords_top.set_upstream(hbase_tables)

    social_receiving = \
        DockerBashOperator(task_id='SocialReceiving',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/social-receiving.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_social_receiving,add_totals_to_social_receiving,top_site,top_site_paid'''
                           )

    social_receiving.set_upstream(site_country_special_referrer_distribution)
    social_receiving.set_upstream(hbase_tables)

    sending_pages = \
        DockerBashOperator(task_id='SendingPages',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/sending-pages.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p estimate_sending_pages,add_totals_to_sending_pages,prepare_sending_pages_social,top_values_sending_pages_social'''
                           )

    sending_pages.set_upstream(estimate_incoming)
    sending_pages.set_upstream(hbase_tables)

    info = \
        DockerBashOperator(task_id='Info',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p create_info_table'''
                           )

    info.set_upstream(hbase_tables)

    ranks = \
        DockerBashOperator(task_id='Ranks',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calculate_ranks,export_top_lists,topsites_for_testing'''
                           )

    ranks.set_upstream(monthly_sum_estimation_parameters)
    ranks.set_upstream(hbase_tables)
    ranks.set_upstream(info)

    misc = \
        DockerBashOperator(task_id='Misc',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/misc.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p calculate_subdomains,insert_worldwide_traffic,insert_daily_data'''
                           )

    misc.set_upstream(monthly_sum_estimation_parameters)
    misc.set_upstream(hbase_tables)

    therest_map = \
        DummyOperator(task_id='TherestMap',
                      dag=dag
                      )

    therest_map.set_upstream(incoming)
    therest_map.set_upstream(outgoing)
    therest_map.set_upstream(keywords_prepare)
    therest_map.set_upstream(keywords_paid)
    therest_map.set_upstream(keywords_organic)
    therest_map.set_upstream(keywords_top)
    therest_map.set_upstream(social_receiving)
    therest_map.set_upstream(sending_pages)
    therest_map.set_upstream(info)
    therest_map.set_upstream(ranks)
    therest_map.set_upstream(misc)

    export_rest = \
        DockerBashOperator(task_id='ExportRest',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p export_rest'''
                           )

    export_rest.set_upstream(therest_map)

    mobile = \
        DockerBashOperator(task_id='Mobile',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/mobile.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )

    mobile.set_upstream(therest_map)
    mobile.set_upstream(export_rest)

    check_customers_est = \
        DockerBashOperator(task_id='CheckCustomersEst',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/qa/checkCustomerEstimationPerSite.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )

    check_customers_est.set_upstream(desktop_daily_estimation)

    check_customer_distros = \
        DockerBashOperator(task_id='CheckCustomerDistros',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/qa/checkCustomerSiteDistro.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )

    check_customer_distros.set_upstream(traffic_distro)

    info_lite = \
        DockerBashOperator(task_id='InfoLite',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p create_info_lite'''
                           )

    info_lite.set_upstream(hbase_tables)

    sites_lite = \
        DockerBashOperator(task_id='SitesLite',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/sites-lite.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )

    sites_lite.set_upstream(therest_map)

    industry_analysis = \
        DockerBashOperator(task_id='IndustryAnalysis',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/categories.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )

    industry_analysis.set_upstream(therest_map)
    industry_analysis.set_upstream(export_rest)

    popular_pages = \
        DockerBashOperator(task_id='PopularPages',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/popular-pages.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )

    popular_pages.set_upstream(hbase_tables)


    return dag


globals()['dag_apps_moving_window_snapshot'] = generate_dags(SNAPHOT_MODE)
globals()['dag_apps_moving_window_daily'] = generate_dags(WINDOW_MODE)
