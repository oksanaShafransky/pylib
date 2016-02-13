__author__ = 'Felix'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.sensors import HdfsSensor

from sw.airflow.operators import DockerCopyHbaseTableOperator
from sw.airflow.docker_bash_operator import DockerBashOperator
from sw.airflow.key_value import *

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_DOCKER = 'mrp'
DEFAULT_HDFS = 'mrp'
DEFAULT_CLUSTER = 'mrp'
CHECK_DATA_PROBLEM_NUM = '20'
DEPLOY_TO_PROD = True

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 01, 21),
    'depends_on_past': False,
    'email': ['felixv@similarweb.com', 'jonathan@similarweb.com', 'yotamg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER, 'hdfs': DEFAULT_HDFS,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER,
                       'problem_num': CHECK_DATA_PROBLEM_NUM}

dag = DAG(dag_id='Scraping_ProcessMobileScraping', default_args=dag_args, params=dag_template_params)



# define jobs

init = DockerBashOperator(task_id='InitResources',
                          dag=dag,
                          docker_name=DEFAULT_DOCKER,
                          bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/init.sh -d {{ ds }} -et {{ params.run_environment }}'''
                          )

wrap_up = DummyOperator(task_id='FinishProcess',
                        dag=dag)

deploy_prod = DummyOperator(task_id='deploy_prod', dag=dag)
deploy_prod_done = DummyOperator(task_id='deploy_prod_done', dag=dag)

deploy_prod_done.set_upstream(deploy_prod)
deploy_prod_done.set_downstream(wrap_up)

#################################################
###    Ranks Related Jobs                       #
#################################################

ps_ranks_exp = DockerBashOperator(task_id='ExportPlaystoreRanks',
                                  dag=dag,
                                  docker_name=DEFAULT_DOCKER,
                                  bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/ranks.sh -d {{ ds }} -p export_playstore -fd'''
                                  )
# ps_ranks_exp.set_downstream(wrap_up)

it_ranks_exp = DockerBashOperator(task_id='ExportiTunesRanks',
                                  dag=dag,
                                  docker_name=DEFAULT_DOCKER,
                                  bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/ranks.sh -d {{ ds }} -p export_itunes -fd'''
                                  )
# it_ranks_exp.set_downstream(wrap_up)

ps_rank_hist = DockerBashOperator(task_id='AssemblePlaystoreRanksHistory',
                                  dag=dag,
                                  docker_name=DEFAULT_DOCKER,
                                  bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/ranks.sh -d {{ ds }} -p playstore_rank_hist''',
                                  priority_weight=3
                                  )
ps_rank_hist.set_upstream(init)
ps_rank_hist.set_upstream(ps_ranks_exp)
ps_rank_hist.set_downstream(deploy_prod)

it_rank_hist = DockerBashOperator(task_id='AssembleiTunesRanksHistory',
                                  dag=dag,
                                  docker_name=DEFAULT_DOCKER,
                                  bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/ranks.sh -d {{ ds }} -p itunes_rank_hist''',
                                  priority_weight=3
                                  )
it_rank_hist.set_upstream(init)
it_rank_hist.set_upstream(it_ranks_exp)
it_rank_hist.set_downstream(deploy_prod)

store_cat_ranks = DockerBashOperator(task_id='StoreCategoryRanks',
                                     dag=dag,
                                     docker_name=DEFAULT_DOCKER,
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/ranks.sh -d {{ ds }} -p store_cat_ranks'''
                                     )
store_cat_ranks.set_upstream(init)
store_cat_ranks.set_upstream(ps_ranks_exp)
store_cat_ranks.set_upstream(it_ranks_exp)
store_cat_ranks.set_downstream(deploy_prod)

trends_7_days = DockerBashOperator(task_id='7DayTrends',
                                   dag=dag,
                                   docker_name=DEFAULT_DOCKER,
                                   bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/trends.sh -d {{ ds }} -td 7''',
                                   priority_weight=2
                                   )
trends_7_days.set_upstream(init)
trends_7_days.set_upstream(ps_ranks_exp)
trends_7_days.set_upstream(it_ranks_exp)
trends_7_days.set_downstream(deploy_prod)

trends_28_days = DockerBashOperator(task_id='28DayTrends',
                                    dag=dag,
                                    docker_name=DEFAULT_DOCKER,
                                    bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/trends.sh -d {{ ds }} -td 28''',
                                    priority_weight=2
                                    )
trends_28_days.set_upstream(init)
trends_28_days.set_upstream(ps_ranks_exp)
trends_28_days.set_upstream(it_ranks_exp)
trends_28_days.set_downstream(deploy_prod)



#################################################
###    Info Related Jobs                        #
#################################################

ps_info = DockerBashOperator(task_id='GatherPlaystoreInfo',
                             dag=dag,
                             docker_name=DEFAULT_DOCKER,
                             bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/app_info.sh -d {{ ds }} -p export_playstore -fd'''
                             )
# ps_info.set_downstream(wrap_up)
ps_info.set_downstream(store_cat_ranks)

it_info = DockerBashOperator(task_id='GatheriTunesInfo',
                             dag=dag,
                             docker_name=DEFAULT_DOCKER,
                             bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/app_info.sh -d {{ ds }} -p export_itunes -fd'''
                             )
# it_info.set_downstream(wrap_up)
it_info.set_downstream(store_cat_ranks)

if DEPLOY_TO_PROD:
    export_app_info = DockerBashOperator(task_id='ExportAppInfo',
                                         dag=dag,
                                         docker_name=DEFAULT_DOCKER,
                                         bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/app_info.sh -d {{ ds }} \
                                                            -p export_couchbase,export_elastic -et {{ params.run_environment }}'''
                                         )
    export_app_info.set_upstream(init)
    export_app_info.set_upstream(ps_info)
    export_app_info.set_upstream(it_info)
    export_app_info.set_downstream(wrap_up)

export_app_info_stage = DockerBashOperator(task_id='ExportAppInfoStage',
                                           dag=dag,
                                           docker_name=DEFAULT_DOCKER,
                                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/app_info.sh -d {{ ds }} \
                                                        -p export_couchbase,export_elastic,export_hbase -et STAGE'''
                                           )
export_app_info_stage.set_upstream(init)
export_app_info_stage.set_upstream(ps_info)
export_app_info_stage.set_upstream(it_info)
export_app_info_stage.set_downstream(wrap_up)

supp_it_info = DockerBashOperator(task_id='SupplementiTunesInfo',
                                  dag=dag,
                                  docker_name=DEFAULT_DOCKER,
                                  bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/app_info.sh -d {{ ds }} -p supp_itunes_info -fd''',
                                  priority_weight=3
                                  )
supp_it_info.set_upstream(init)
supp_it_info.set_upstream(it_info)
supp_it_info.set_upstream(it_ranks_exp)
supp_it_info.set_downstream(deploy_prod)


#################################################
###    Keywords Related Jobs                    #
#################################################

playstore_kw_path = '/similargroup/mobile/keywords/playstoreScraped'
itunes_kw_path = '/similargroup/mobile/keywords/itunesScraped'

# We check existence of next day's folder, meaning run day's scraping is done
ps_kw_ready = HdfsSensor(task_id='PlaystoreScrapedKeywordsReady',
                         dag=dag,
                         hdfs_conn_id='hdfs_%s' % DEFAULT_HDFS,
                         filepath='''%s/{{ macros.date_partition(macros.ds_add(ds, 1)) }}''' % playstore_kw_path,
                         execution_timeout=timedelta(minutes=240)
                         )

it_kw_ready = HdfsSensor(task_id='iTunesScrapedKeywordsReady',
                         dag=dag,
                         hdfs_conn_id='hdfs_%s' % DEFAULT_HDFS,
                         filepath='''%s/{{ macros.date_partition(macros.ds_add(ds, 1)) }}''' % itunes_kw_path,
                         execution_timeout=timedelta(minutes=240)
                         )

kw_ranks = DockerBashOperator(task_id='KeywordRanks',
                              dag=dag,
                              docker_name=DEFAULT_DOCKER,
                              bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/keywords.sh -d {{ ds }} -p kw_apps'''
                              )
kw_ranks.set_upstream(init)
kw_ranks.set_upstream(ps_kw_ready)
kw_ranks.set_upstream(it_kw_ready)
kw_ranks.set_downstream(deploy_prod)

app_kw = DockerBashOperator(task_id='AppKeywords',
                            dag=dag,
                            docker_name=DEFAULT_DOCKER,
                            bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/keywords.sh -d {{ ds }} -p app_keywords''',
                            priority_weight=2
                            )
app_kw.set_upstream(init)
app_kw.set_upstream(ps_kw_ready)
app_kw.set_upstream(it_kw_ready)
app_kw.set_downstream(deploy_prod)

app_kw_hist = DockerBashOperator(task_id='AppKeywordRanksHistory',
                                 dag=dag,
                                 docker_name=DEFAULT_DOCKER,
                                 bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/keywords.sh -d {{ ds }} -p app_kw_hist''',
                                 priority_weight=4
                                 )
app_kw_hist.set_upstream(init)
app_kw_hist.set_upstream(ps_kw_ready)
app_kw_hist.set_upstream(it_kw_ready)
app_kw_hist.set_downstream(deploy_prod)

#################################################
###    Lite Related Jobs                        #
#################################################

link = DockerBashOperator(task_id='LinkAppsWithSites',
                          dag=dag,
                          docker_name=DEFAULT_DOCKER,
                          bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/link.sh -d {{ ds }} -p link_app_devsite,link_pub_site,link_app_pub,store_app_sites -et {{ params.run_environment }}'''
                          )
link.set_upstream(init)
link.set_upstream(ps_info)
link.set_upstream(it_info)
link.set_downstream(deploy_prod)

if DEPLOY_TO_PROD:
    link_prod = DockerBashOperator(task_id='ExportLink',
                                   dag=dag,
                                   docker_name=DEFAULT_DOCKER,
                                   bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/link.sh -d {{ ds }}  -p export_site_apps -et {{ params.run_environment }}'''
                                   )
    link_prod.set_upstream(init)
    link_prod.set_upstream(ps_info)
    link_prod.set_upstream(it_info)
    link_prod.set_upstream(link)
    link_prod.set_downstream(deploy_prod)

link_stage = DockerBashOperator(task_id='LinkAppsWithSitesStage',
                                dag=dag,
                                docker_name=DEFAULT_DOCKER,
                                bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/link.sh -d {{ ds }} -p export_site_apps -et STAGE'''
                                )
link_stage.set_upstream(init)
link_stage.set_upstream(ps_info)
link_stage.set_upstream(it_info)
link_stage.set_upstream(link)
link_stage.set_downstream(deploy_prod)

lite_app_info = DockerBashOperator(task_id='CopyAppDetailsForLite',
                                   dag=dag,
                                   docker_name=DEFAULT_DOCKER,
                                   bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/lite.sh -d {{ ds }} -p details'''
                                   )
lite_app_info.set_upstream(init)
lite_app_info.set_upstream(ps_info)
lite_app_info.set_upstream(it_info)
lite_app_info.set_upstream(supp_it_info)
lite_app_info.set_upstream(export_app_info_stage)
lite_app_info.set_downstream(deploy_prod)

lite_app_stats = DockerBashOperator(task_id='CopyAppStatsForLite',
                                    dag=dag,
                                    docker_name=DEFAULT_DOCKER,
                                    bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/lite.sh -d {{ ds }} -p stats'''
                                    )
lite_app_stats.set_upstream(init)
lite_app_stats.set_downstream(deploy_prod)

lite_kw = DockerBashOperator(task_id='CopyKeywordsForLite',
                             dag=dag,
                             docker_name=DEFAULT_DOCKER,
                             bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/lite.sh -d {{ ds }} -p keywords'''
                             )
lite_kw.set_upstream(init)
lite_kw.set_upstream(ps_kw_ready)
lite_kw.set_upstream(it_kw_ready)
lite_kw.set_downstream(deploy_prod)

#################################################
###    CheckData                                #
#################################################

check_data = DockerBashOperator(task_id='CheckDataBeforeProd',
                                dag=dag,
                                docker_name=DEFAULT_DOCKER,
                                bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/checkScrapeDataBeforeProd.sh -d {{ ds }} {{ params.problem_num }}'''
                                )

check_data.set_upstream(deploy_prod)
check_data.set_downstream(deploy_prod_done)



#################################################
###    Deploy                                  #
#################################################
last_deploy_step = None
for target_cluster in ('hbp1','hbp2'):
    copy_app_details = DockerCopyHbaseTableOperator(
        task_id='copy_app_details_%s' % target_cluster,
        dag=dag,
        docker_name=DEFAULT_DOCKER,
        source_cluster=DEFAULT_CLUSTER,
        target_cluster=target_cluster,
        table_name_template="app_details_{{ macros.ds_format(ds, '%Y-%m-%d', '%y_%m_%d') }}"
    )
    copy_app_details.set_upstream(deploy_prod)
    copy_app_details.set_downstream(deploy_prod_done)
    if last_deploy_step is not None:
        copy_app_details.set_upstream(last_deploy_step)

    copy_app_top_list = DockerCopyHbaseTableOperator(
        task_id='copy_app_top_list_%s' % target_cluster,
        dag=dag,
        docker_name=DEFAULT_DOCKER,
        source_cluster=DEFAULT_CLUSTER,
        target_cluster=target_cluster,
        table_name_template="app_top_list_{{ macros.ds_format(ds, '%Y-%m-%d', '%y_%m_%d') }}"
    )
    copy_app_top_list.set_upstream(deploy_prod)
    copy_app_top_list.set_downstream(deploy_prod_done)
    if last_deploy_step is not None:
        copy_app_top_list.set_upstream(last_deploy_step)

    copy_app_cat_rank = DockerCopyHbaseTableOperator(
        task_id='copy_app_cat_rank_%s' % target_cluster,
        dag=dag,
        docker_name=DEFAULT_DOCKER,
        source_cluster=DEFAULT_CLUSTER,
        target_cluster=target_cluster,
        table_name_template="app_cat_rank_{{ macros.ds_format(ds, '%Y-%m-%d', '%y_%m_%d') }}"
    )
    copy_app_cat_rank.set_upstream(deploy_prod)
    copy_app_cat_rank.set_downstream(deploy_prod_done)
    if last_deploy_step is not None:
        copy_app_cat_rank.set_upstream(last_deploy_step)

    copy_mobile_app_keyword_positions = DockerCopyHbaseTableOperator(
        task_id='copy_mobile_app_keyword_positions_%s' % target_cluster,
        dag=dag,
        docker_name=DEFAULT_DOCKER,
        source_cluster=DEFAULT_CLUSTER,
        target_cluster=target_cluster,
        table_name_template="mobile_app_keyword_positions_{{ macros.ds_format(ds, '%Y-%m-%d', '%y_%m_%d') }}"
    )
    copy_mobile_app_keyword_positions.set_upstream(deploy_prod)
    copy_mobile_app_keyword_positions.set_downstream(deploy_prod_done)
    if last_deploy_step is not None:
        copy_mobile_app_keyword_positions.set_upstream(last_deploy_step)

    copy_app_lite = DockerCopyHbaseTableOperator(
        task_id='copy_app_lite_%s' % target_cluster,
        dag=dag,
        docker_name=DEFAULT_DOCKER,
        source_cluster=DEFAULT_CLUSTER,
        target_cluster=target_cluster,
        table_name_template="app_lite_{{ macros.ds_format(ds, '%Y-%m-%d', '%y_%m_%d') }}"
    )
    copy_app_lite.set_upstream(deploy_prod)
    copy_app_lite.set_downstream(deploy_prod_done)
    if last_deploy_step is not None:
        copy_app_lite.set_upstream(last_deploy_step)

    last_deploy_step = DummyOperator(task_id='deploy_step_%s' % target_cluster, dag=dag)
    last_deploy_step.set_upstream(copy_app_details)
    last_deploy_step.set_upstream(copy_app_top_list)
    last_deploy_step.set_upstream(copy_app_cat_rank)
    last_deploy_step.set_upstream(copy_mobile_app_keyword_positions)
    last_deploy_step.set_upstream(copy_app_lite)
    last_deploy_step.set_downstream(deploy_prod_done)




#################################################
###    Wrap Up                                  #
#################################################

register_success = KeyValueSetOperator(task_id='RegisterSuccessOnETCD',
                                       dag=dag,
                                       path='''services/process_mobile_scraping/success/{{ ds }}''',
                                       env='PRODUCTION'
                                       )
register_success.set_upstream(wrap_up)

register_success_stage = KeyValueSetOperator(task_id='RegisterSuccessOnETCDStage',
                                             dag=dag,
                                             path='''services/process_mobile_scraping/success/{{ ds }}''',
                                             env='STAGING'
)
register_success_stage.set_upstream(wrap_up)

update_latest_date = KeyValuePromoteOperator(task_id='SetLatestDate',
                                             dag=dag,
                                             path='services/dynamic_settings/window/mobile_scraper_date',
                                             value='''{{ ds }}''',
                                             env='PRODUCTION'
                                             )
update_latest_date.set_upstream(wrap_up)

update_latest_date_stage = KeyValuePromoteOperator(task_id='SetLatestDateStage',
                                                   dag=dag,
                                                   path='services/dynamic_settings/window/mobile_scraper_date',
                                                   value='''{{ ds }}''',
                                                   env='STAGING'
)
update_latest_date_stage.set_upstream(wrap_up)

register_available = KeyValueSetOperator(task_id='SetDataAvailableDate',
                                         dag=dag,
                                         path='''services/process_mobile_scraping/data-available/{{ ds }}''',
                                         env='PRODUCTION'
                                         )
register_available.set_upstream(wrap_up)

register_available_stage = KeyValueSetOperator(task_id='SetDataAvailableDateStage',
                                               dag=dag,
                                               path='''services/process_mobile_scraping/data-available/{{ ds }}''',
                                               env='STAGING'
)
register_available_stage.set_upstream(wrap_up)

if DEPLOY_TO_PROD:
    update_elastic_alias = DockerBashOperator(task_id='UpdateElasticAlias',
                                              dag=dag,
                                              docker_name=DEFAULT_DOCKER,
                                              bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/elastic.sh -d {{ ds }} -et {{ params.run_environment }}'''
                                              )
    update_elastic_alias.set_upstream(wrap_up)

update_elastic_alias_stage = DockerBashOperator(task_id='UpdateElasticAliasStage',
                                                dag=dag,
                                                docker_name=DEFAULT_DOCKER,
                                                bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/elastic.sh -d {{ ds }} -et STAGE'''
                                                )
update_elastic_alias_stage.set_upstream(wrap_up)
update_elastic_alias_stage.set_downstream(register_success)
update_elastic_alias_stage.set_downstream(update_latest_date)
update_elastic_alias_stage.set_downstream(register_available)

#################################################
###    Cleanup                                  #
#################################################


cleanup_interval_start = 10
cleanup_interval_end = 3  # this is effectively the retention policy, in days

# it may be necessary to separate retention policies for different data components, which is supported by the underlying script
# for now, keep everything the same
cleanups = []
cleanups_stage = []
idx = 0
for days_back in range(cleanup_interval_start, cleanup_interval_end, -1):
    cleanups += [DockerBashOperator(task_id='''Cleanup_%d_days''' % days_back,
                                    dag=dag,
                                    docker_name=DEFAULT_DOCKER,
                                    bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/cleanup.sh -d {{ macros.ds_add(ds, -%d) }} -et {{ params.run_environment }}''' % days_back
                                    )
                 ]
    cleanups[idx].set_upstream(wrap_up)
    cleanups[idx].set_upstream(update_elastic_alias_stage)

    unregister_available = KeyValueDeleteOperator(task_id='DropDataAvailableDate%dDaysBack' % days_back,
                                                  dag=dag,
                                                  path='''services/process_mobile_scraping/data-available/{{ macros.ds_add(ds, -%d) }}''' % days_back,
                                                  env='PRODUCTION'
                                                  )
    unregister_available.set_upstream(cleanups[idx])

    cleanups_stage += [DockerBashOperator(task_id='''Cleanup_%d_days_Stage''' % days_back,
                                          dag=dag,
                                          docker_name=DEFAULT_DOCKER,
                                          bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/cleanup.sh -d {{ macros.ds_add(ds, -%d) }} -et STAGE''' % days_back
    )
    ]
    cleanups_stage[idx].set_upstream(wrap_up)
    cleanups_stage[idx].set_upstream(update_elastic_alias_stage)

    unregister_available_stage = KeyValueDeleteOperator(task_id='DropDataAvailableDate%dDaysBackStage' % days_back,
                                                        dag=dag,
                                                        path='''services/process_mobile_scraping/data-available/{{ macros.ds_add(ds, -%d) }}''' % days_back,
                                                        env='STAGING'
    )
    unregister_available_stage.set_upstream(cleanups_stage[idx])

    idx += 1

#################################################
###    Lite Sitemap                             #
#################################################

if DEPLOY_TO_PROD:
    lite_sitemap = DockerBashOperator(task_id='LiteSitemap',
                                      dag=dag,
                                      docker_name=DEFAULT_DOCKER,
                                      bash_command='''{{ params.execution_dir }}/mobile/scripts/app-store/lite_sitemap.sh'''
                                      )

    lite_sitemap.set_upstream(wrap_up)
