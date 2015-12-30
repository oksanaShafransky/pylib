__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator
from sw.airflow.operators import DockerBashSensor
from sw.airflow.operators import  DockerCopyHbaseTableOperator
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
DEFAULT_HBASE_CLUSTER = 'hbp1'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=4)
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
        dag_args_for_mode.update({'start_date': datetime(2015, 12, 11)})

    if is_snapshot_dag():
        dag_args_for_mode.update({'start_date': datetime(2015, 8, 31), 'end_date': datetime(2015, 8, 31)})

    dag_template_params_for_mode = dag_template_params.copy()
    if is_window_dag():
        dag_template_params_for_mode.update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})

    if is_snapshot_dag():
        dag_template_params_for_mode.update({'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

    dag = DAG(dag_id='AndroidFixAugustMobileAppsMovingWindow_' + mode, default_args=dag_args_for_mode, params=dag_template_params_for_mode,
              #schedule_interval=(timedelta(days=1)) if (is_window_dag()) else '0 0 l * *')
              #Following is temporary hack until we upgrade to Airflow 1.6.x or later
              schedule_interval=timedelta(days=1))

    mobile_daily_estimation = ExternalTaskSensor(external_dag_id='MobileDailyEstimation',
                                                 dag=dag,
                                                 task_id='MobileDailyEstimation',
                                                 external_task_id='MobileAppsDailyEstimation')

    ##################
    # App Engagement #
    ##################

    app_engagement = \
        DockerBashOperator(task_id='AppEngagement',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/engagement.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }} -f'''
                           )
    app_engagement.set_upstream([mobile_daily_estimation])

    if is_window_dag():
        app_engagement_sanity_check = \
            DockerBashOperator(task_id='AppEngagementSanityCheck',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/qa/checkAppAndCountryEngagementEstimation.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -p check_window -f'''
                               )
        app_engagement_sanity_check.set_upstream(app_engagement)


    #############
    # App Ranks #
    #############

    usage_ranks_main = \
        DockerBashOperator(task_id='UsageRanksMain',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/ranks.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }} -f'''
                           )
    usage_ranks_main.set_upstream([app_engagement])

    usage_ranks = DummyOperator(task_id='UsageRanks',
                                dag=dag
                                )

    usage_ranks.set_upstream(usage_ranks_main)

    prepare_ranks = \
        DockerBashOperator(task_id='PrepareRanks',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/cross_cache.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }} -p prepare_app_rank_export -f'''
                           )
    prepare_ranks.set_upstream(usage_ranks)

    #ranks_export_stage = \
    #    DockerBashOperator(task_id='RanksExportStage',
    #                       dag=dag,
    #                       docker_name='''{{ params.cluster }}''',
    #                       bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/cross_cache.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }} -et STAGE -p export'''
    #                       )
    #ranks_export_stage.set_upstream(prepare_ranks)

    #TODO add check that this is indeed prod environment
    #if is_prod_env():
    #    ranks_export_prod = \
    #        DockerBashOperator(task_id='RanksExportProd',
    #                           dag=dag,
    #                          docker_name='''{{ params.cluster }}''',
    #                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/cross_cache.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -env main -m {{ params.mode }} -mt {{ params.mode_type }} -et PRODUCTION -p export'''
    #                           )
    #    ranks_export_prod.set_upstream(prepare_ranks)

    #export_ranks = DummyOperator(task_id='ExportRanks',
    #                             dag=dag
    #                             )

    #if is_prod_env():
    #    export_ranks.set_upstream([ranks_export_prod, ranks_export_stage, prepare_ranks])
    #else:
    #    export_ranks.set_upstream([ranks_export_stage, prepare_ranks])

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
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/trends.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -td 28 -f'''
                               )
        trends_28_days.set_upstream(usage_ranks)

        # TODO configure parallelism setting for this task, which is heavier (20 slots)
        trends_7_days = \
            DockerBashOperator(task_id='Trends7Days',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/trends.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -td 7 -f'''
                               )
        trends_7_days.set_upstream([usage_ranks])
        trends.set_upstream([trends_28_days, trends_7_days])

    if is_snapshot_dag():
        trends_1_month = \
            DockerBashOperator(task_id='Trends1Month',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/trends.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -td 1 -f'''
                               )
        trends_1_month.set_upstream(usage_ranks)
        trends.set_upstream(trends_1_month)

    ####################
    # Dynamic Settings #
    ####################

    apps = DummyOperator(task_id='Apps',
                         dag=dag
                         )
    apps.set_upstream([app_engagement, usage_ranks, trends])

    #######################
    # Top Apps for Sanity #
    #######################

    if is_snapshot_dag():
        top_apps_for_sanity = \
            DockerBashOperator(task_id='TopAppsForSanity',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/qaTopApps.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -f'''
                               )
        top_apps_for_sanity.set_upstream(usage_ranks)


    deploy_targets = ['hbp1', 'hbp2']
    ################
    # Copy to Prod #
    ################

    hbase_suffix_template = ('''{{ params.mode_type }}_{{ macros.ds_format(ds, "%Y-%m-%d", "%y_%m_%d")}}''' if is_window_dag() else
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
                    table_name_template='app_sdk_stats_' + hbase_suffix_template
            )
        copy_to_prod_app_sdk.set_upstream([app_engagement])

    #    copy_to_prod_cats = \
    #        DockerCopyHbaseTableOperator(
    #                task_id='CopyToProdCats',
    #                dag=dag,
    #                docker_name='''{{ params.cluster }}''',
    #                source_cluster='mrp',
    #                target_cluster=','.join(deploy_targets),
    #                table_name_template='app_sdk_category_stats_' + hbase_suffix_template
    #        )
    #    copy_to_prod_cats.set_upstream([app_engagement, category_retention_store, usage_pattern_categories])

        copy_to_prod_leaders = \
            DockerCopyHbaseTableOperator(
                    task_id='CopyToProdLeaders',
                    dag=dag,
                    docker_name='''{{ params.cluster }}''',
                    source_cluster='mrp',
                    target_cluster=','.join(deploy_targets),
                    table_name_template='app_sdk_category_lead_' + hbase_suffix_template
            )
        copy_to_prod_leaders.set_upstream([app_engagement])

        copy_to_prod_engage = \
            DockerCopyHbaseTableOperator(
                    task_id='CopyToProdEngage',
                    dag=dag,
                    docker_name='''{{ params.cluster }}''',
                    source_cluster='mrp',
                    target_cluster=','.join(deploy_targets),
                    table_name_template='app_eng_rank_' + hbase_suffix_template
            )
        copy_to_prod_engage.set_upstream(usage_ranks)

        copy_to_prod_rank = \
            DockerCopyHbaseTableOperator(
                    task_id='CopyToProdRank',
                    dag=dag,
                    docker_name='''{{ params.cluster }}''',
                    source_cluster='mrp',
                    target_cluster=','.join(deploy_targets),
                    table_name_template='cat_mod_app_rank_' + hbase_suffix_template
            )
        copy_to_prod_rank.set_upstream([usage_ranks, trends])

        copy_to_prod.set_upstream([copy_to_prod_app_sdk, copy_to_prod_leaders, copy_to_prod_engage, copy_to_prod_rank])

    return dag


globals()['android_fix_dag_august_apps_moving_window_snapshot'] = generate_dags(SNAPHOT_MODE)