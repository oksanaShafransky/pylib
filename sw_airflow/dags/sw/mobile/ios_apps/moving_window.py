__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from sw.airflow.key_value import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/ios-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
WINDOW_MODE = 'window'
SNAPHOT_MODE = 'snapshot'
WINDOW_MODE_TYPE = 'last-28'
SNAPSHOT_MODE_TYPE = 'monthly'
IS_PROD = True

dag_args = {
    'owner': 'similarweb',
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION',
                       'cluster': DEFAULT_CLUSTER}


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

    #TODO insert the real logic here
    def is_prod_env():
        return IS_PROD

    dag_args_for_mode = dag_args.copy()
    if is_window_dag():
        dag_args_for_mode.update({'start_date': datetime(2016, 1, 28)})

    if is_snapshot_dag():
        dag_args_for_mode.update({'start_date': datetime(2016, 1, 1), 'end_date': datetime(2016, 1, 1)})

    dag_template_params_for_mode = dag_template_params.copy()
    if is_window_dag():
        dag_template_params_for_mode.update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})

    if is_snapshot_dag():
        dag_template_params_for_mode.update({'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

    dag = DAG(dag_id='IosApps_' + mode_dag_name(), default_args=dag_args_for_mode, params=dag_template_params_for_mode,
              schedule_interval="@daily" if is_window_dag() else "@monthly")

    mobile_estimation = ExternalTaskSensor(external_dag_id='IosApps_Estimation',
                                           dag=dag,
                                           task_id='DailyEstimation',
                                           external_task_id='Estimation')

    preliminary = ExternalTaskSensor(external_dag_id='IosApps_Preliminary',
                                              dag=dag,
                                              task_id='Preliminary',
                                              external_task_id='Preliminary')

    # for now, wait for tables to be created by the android window

    hbase_tables_ready = \
        ExternalTaskSensor(external_dag_id='AndroidApps_%s' % mode_dag_name(),
                           dag=dag,
                           task_id='PrepareHBaseTables',
                           external_task_id='PrepareHBaseTables'
                           )

    ##################
    # App Engagement #
    ##################

    app_engagement = \
        DockerBashOperator(task_id='AppEngagement',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/ios/aggregation.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -env all_countries -m {{ params.mode }} -mt {{ params.mode_type }}'''
                           )
    app_engagement.set_upstream([mobile_estimation, hbase_tables_ready])

    # Todo: Fix this task whose data was deleted fue to retention by fallbacking into using snapshot's data
    #if is_window_dag():
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

    calc_ranks = \
        DockerBashOperator(task_id='CalculateUsageRanks',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -hdb mobile -m {{ params.mode }} -mt {{ params.mode_type }} -fs -p join_scores_info,cat_ranks'''
                           )
    calc_ranks.set_upstream(app_engagement)

    top_list_store = \
        DockerBashOperator(task_id='StoreUsageRanks',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -hdb mobile -m {{ params.mode }} -mt {{ params.mode_type }} -p store_cat_ranks'''
                           )
    top_list_store.set_upstream(calc_ranks)

    app_ranks_histogram_store = \
        DockerBashOperator(task_id='RecordAppUsageRanksHistory',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-engagement/ranks.sh -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} -bd {{ params.base_hdfs_dir }} -hdb mobile -m {{ params.mode }} -mt {{ params.mode_type }} -p store_app_ranks'''
                           )
    app_ranks_histogram_store.set_upstream(calc_ranks)

    usage_ranks = DummyOperator(task_id='UsageRanks',
                                dag=dag
                                )
    usage_ranks.set_upstream([calc_ranks, top_list_store, app_ranks_histogram_store])

    # TODO: cleanup on HDFS

    #############################
    # Update Availability Dates #
    #############################

    register_available = KeyValueSetOperator(task_id='MarkDataAvailability',
                                             dag=dag,
                                             path='''services/ios/{{ params.mode }}/data-available/{{ ds }}''',
                                             env='''{{ params.run_environment }}'''
                                             )
    register_available.set_upstream(app_engagement)
    register_available.set_upstream(usage_ranks)

    #################
    # Histograms    #
    #################

    """
    app_sdk_hist_sensor = \
        HdfsSensor(task_id='AppSdkStatsHistogramReady',
                   dag=dag,
                   hdfs_conn_id='hdfs_%s' % DEFAULT_CLUSTER,
                   filepath='''{{ params.base_hdfs_dir }}/{{ params.mode }}/histogram/type={{ params.mode_type }}/{{ macros.date_partition(ds) }}/app-sdk-stats/_SUCCESS''',
                   execution_timeout=timedelta(minutes=600)
                   )

    app_sdk_hist_register =  \
        DockerBashOperator(task_id='StoreAppSdkTableSplits',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''source {{ params.execution_dir }}/scripts/common.sh && \
                                           hadoopexec {{ params.execution_dir }}/mobile mobile.jar com.similargroup.common.job.topvalues.KeyHistogramAnalysisUtil \
                                           -in {{ params.base_hdfs_dir }}/{{ params.mode }}/histogram/type={{ params.mode_type }}/{{ macros.date_partition(ds) }}/app-sdk-stats \
                                           -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} \
                                           -k 500000 \
                                           -t app_sdk_stats{{ macros.hbase_table_suffix_partition(ds, params.mode, params.mode_type) }}
                                        '''
                           )
    app_sdk_hist_register.set_upstream(app_sdk_hist_sensor)

    app_eng_rank_hist_sensor = \
        HdfsSensor(task_id='AppRanksHistogramReady',
                   dag=dag,
                   hdfs_conn_id='hdfs_%s' % DEFAULT_CLUSTER,
                   filepath='''{{ params.base_hdfs_dir }}/{{ params.mode }}/histogram/type={{ params.mode_type }}/{{ macros.date_partition(ds) }}/app-eng-rank/_SUCCESS''',
                   execution_timeout=timedelta(minutes=600)
                   )

    app_eng_rank_hist_register =  \
        DockerBashOperator(task_id='StoreAppRanksTableSplits',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''source {{ params.execution_dir }}/scripts/common.sh && \
                                           hadoopexec {{ params.execution_dir }}/mobile mobile.jar com.similargroup.common.job.topvalues.KeyHistogramAnalysisUtil \
                                           -in {{ params.base_hdfs_dir }}/{{ params.mode }}/histogram/type={{ params.mode_type }}/{{ macros.date_partition(ds) }}/app-eng-rank \
                                           -d {{ macros.last_interval_day(ds, dag.schedule_interval) }} \
                                           -k 500000 \
                                           -t app_sdk_stats{{ macros.hbase_table_suffix_partition(ds, params.mode, params.mode_type) }}
                                        '''
                           )
    app_eng_rank_hist_register.set_upstream(app_eng_rank_hist_sensor)
    """

    return dag


globals()['dag_ios_apps_moving_window_snapshot'] = generate_dag(SNAPHOT_MODE)
globals()['dag_ios_apps_moving_window_daily'] = generate_dag(WINDOW_MODE)
