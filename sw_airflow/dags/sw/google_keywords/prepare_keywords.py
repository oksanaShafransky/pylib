__author__ = 'lajonat'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.operators import DockerCopyHbaseTableOperator
from sw.airflow.docker_bash_operator import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_DOCKER = 'mrp'
DEFAULT_HDFS = 'mrp'
DEFAULT_CLUSTER = 'mrp'
DEPLOY_TO_PROD = True

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production-mrp'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2016, 01, 21),
    'depends_on_past': False,
    'email': ['felixv@similarweb.com', 'jonathan@similarweb.com', 'yotamg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER, 'hdfs': DEFAULT_HDFS,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='Scraping_PrepareGoogleKeywords', default_args=dag_args, params=dag_template_params,
          schedule_interval="@monthly")



# define jobs

splits = DockerBashOperator(task_id='GetKeywordSplits',
                            dag=dag,
                            docker_name=DEFAULT_DOCKER,
                            bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/scraped-keywords.sh -d {{ ds }} -p get_keyword_splits'''
                            )

init = DockerBashOperator(task_id='InitResources',
                          dag=dag,
                          docker_name=DEFAULT_DOCKER,
                          bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ macros.ds_add(macros.last_day_of_month(ds),2) }} -p tables'''
                          )
init.set_upstream(splits)

wrap_up = DummyOperator(task_id='FinishProcess',
                        dag=dag)

deploy_prod = DummyOperator(task_id='deploy_prod', dag=dag)
deploy_prod_done = DummyOperator(task_id='deploy_prod_done', dag=dag)

deploy_prod_done.set_upstream(deploy_prod)
deploy_prod_done.set_downstream(wrap_up)

get_next_keywords = DockerBashOperator(task_id='GetNewKeywords',
                                       dag=dag,
                                       docker_name=DEFAULT_DOCKER,
                                       bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/scraped-keywords.sh -d {{ ds }} -p calculate_top_sites,calculate_keywords,save_keywords'''
                                       )
get_next_keywords.set_downstream(wrap_up)
get_next_keywords.set_upstream(init)

process = DockerBashOperator(task_id='ProcessKeywords',
                             dag=dag,
                             docker_name=DEFAULT_DOCKER,
                             bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/scraped-keywords.sh -d {{ ds }} -p process'''
                             )
process.set_downstream(deploy_prod)
process.set_upstream(init)

#################################################
###    Deploy                                  #
#################################################
if DEPLOY_TO_PROD:
    last_deploy_step = None
    for target_cluster in ('hbp1','hbp2'):
        copy_processed = DockerCopyHbaseTableOperator(
            task_id='copy_sites_scrape_stat_%s' % target_cluster,
            dag=dag,
            docker_name=DEFAULT_DOCKER,
            source_cluster=DEFAULT_CLUSTER,
            target_cluster=target_cluster,
            table_name_template="sites_scrape_stat_{{ macros.ds_format(macros.ds_add(ds,-1), '%Y-%m-%d', '%y_%m') }}"
        )
        copy_processed.set_upstream(deploy_prod)
        copy_processed.set_downstream(deploy_prod_done)
        if last_deploy_step is not None:
            copy_processed.set_upstream(last_deploy_step)

        last_deploy_step = DummyOperator(task_id='deploy_step_%s' % target_cluster, dag=dag)
        last_deploy_step.set_upstream(copy_processed)
        last_deploy_step.set_downstream(deploy_prod_done)


#################################################
###    Wrap Up                                  #
#################################################

register_success = DockerBashOperator(task_id='RegisterSuccessOnETCD',
                                      dag=dag,
                                      docker_name=DEFAULT_DOCKER,
                                      bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/scraped-keywords.sh -d {{ ds }} -p set_success'''
                                      )
register_success.set_upstream(wrap_up)

register_adwords = DockerBashOperator(task_id='RegisterAdwordsSuccessOnETCD',
                                      dag=dag,
                                      docker_name=DEFAULT_DOCKER,
                                      bash_command='''{{ params.execution_dir }}/analytics/scripts/monthly/scraped-keywords.sh -d {{ ds }} -p set_adwords_success'''
)
register_adwords.set_upstream(wrap_up)