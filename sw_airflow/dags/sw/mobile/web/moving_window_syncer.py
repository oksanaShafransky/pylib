__author__ = 'Amit Rom'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator
from sw.airflow.operators import  DockerCopyHbaseTableOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
WINDOW_MODE = 'window'
SNAPHOT_MODE = 'snapshot'
WINDOW_MODE_TYPE = 'last-28'
SNAPSHOT_MODE_TYPE = 'monthly'
DEFAULT_HBASE_CLUSTER = 'hbp1'

ETCD_ENV_ROOT = {'STAGE': 'v1/staging', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'depends_on_past': False,
    'email': ['amitr@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION',
                       'cluster': DEFAULT_CLUSTER, 'hbase_cluster': DEFAULT_HBASE_CLUSTER}


# TODO: only snapshot for now
def generate_dags(mode):
    def is_window_dag():
        return mode == WINDOW_MODE

    def is_snapshot_dag():
        return mode == SNAPHOT_MODE

    #TODO insert the real logic here
    def is_prod_env():
        return True

    def mode_dag_name():
        if is_window_dag():
            return 'Window'
        if is_snapshot_dag():
            return 'Snapshot'

    dag_args_for_mode = dag_args.copy()
    if is_window_dag():
        dag_args_for_mode.update({'start_date': datetime(2015, 12, 01)})

    if is_snapshot_dag():
        dag_args_for_mode.update({'start_date': datetime(2015, 12, 30), 'end_date': datetime(2015, 12, 30)})

    dag_template_params_for_mode = dag_template_params.copy()
    if is_window_dag():
        dag_template_params_for_mode.update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})

    if is_snapshot_dag():
        dag_template_params_for_mode.update({'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

    dag = DAG(dag_id='MobileWebMovingWindowSyncer_' + mode, default_args=dag_args_for_mode, params=dag_template_params_for_mode,
          #schedule_interval=(timedelta(days=1)) if (is_window_dag()) else '0 0 l * *')
          #Following is temporary hack until we upgrade to Airflow 1.6.x or later
          schedule_interval=timedelta(days=1))

    mobile_web_data_stored = DummyOperator(task_id='MobileWebDataStored', dag=dag)

    # mobile web data
    mobile_web_data = ExternalTaskSensor(external_dag_id='MobileApps_' + mode_dag_name(),
                                                  dag=dag,
                                                  task_id="MobileWebData",
                                                  external_task_id='MobileWeb')
    mobile_web_data_stored.set_upstream(mobile_web_data)

    # TODO: referrals only exists in snapshot for now
    if is_snapshot_dag():
        # mobile web referrals
        mobile_web_referrals_data = ExternalTaskSensor(external_dag_id='MobileWebReferralsMovingWindow_' + mode,
                                                     dag=dag,
                                                     task_id="MobileWebReferralsData",
                                                     external_task_id='FinishProcess')
        mobile_web_data_stored.set_upstream(mobile_web_referrals_data)


    cleanup_from = 8
    cleanup_to = 3

    ##################
    # Clean-Up Stage #
    ##################

    if is_window_dag():

        cleanup_stage = DummyOperator(task_id='CleanupStage',
                                           dag=dag
                                           )

        for i in range(cleanup_to, cleanup_from):
            cleanup_stage_dt_minus_i = \
                DockerBashOperator(task_id='CleanupStage_DT-%s' % i,
                                   dag=dag,
                                   docker_name='''{{ params.cluster }}''',
                                   bash_command='''{{ params.execution_dir }}/mobile/scripts/windowCleanup.sh -d {{ ds_add(ds,-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p delete_files -p drop_hbase_tables -fl mw''' % i
                                   )
            cleanup_stage_dt_minus_i.set_upstream(mobile_web_data_stored)
            cleanup_stage .set_upstream(cleanup_stage_dt_minus_i)

    ################
    # Copy to Prod #
    ################
    deploy_targets = ['hbp1', 'hbp2']
    if is_prod_env():
        hbase_suffix_template = ('''{{ params.mode_type }}_{{ macros.ds_format(ds, "%Y-%m-%d", "%y_%m_%d")}}''' if is_window_dag() else
                                 '''{{macros.ds_format(ds, "%Y-%m-%d", "%y_%m")}}''')

        copy_to_prod = DummyOperator(task_id='CopyToProd',
                                     dag=dag
        )

        copy_to_prod_mw = DockerCopyHbaseTableOperator(
                task_id='CopyToProdMW',
                dag=dag,
                docker_name='''{{ params.cluster }}''',
                source_cluster='mrp',
                target_cluster=','.join(deploy_targets),
                table_name_template='mobile_web_stats_' + hbase_suffix_template
        )

        copy_to_prod_mw.set_upstream(mobile_web_data_stored)
        copy_to_prod.set_upstream(copy_to_prod_mw)

    ################
    # Cleanup Prod #
    ################
    if is_prod_env():
        if is_window_dag():

            cleanup_prod = DummyOperator(task_id='CleanupProd',
                                         dag=dag
                                         )

            for i in range(cleanup_to, cleanup_from):
                #TODO check why is it configured to use this specific docker; extract reference to configuration
                for target in deploy_targets:
                    cleanup_day =  DockerBashOperator(task_id='Cleanup%s_DS-%s' % (target, i),
                                           dag=dag,
                                           docker_name='%s' % target,
                                           bash_command='''{{ params.execution_dir }}/mobile/scripts/windowCleanup.sh -d {{ ds_add(ds,-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -p drop_hbase_tables -fl mw''' % i
                                           )

                    cleanup_day.set_upstream(copy_to_prod)
                    cleanup_prod.set_upstream(cleanup_day)


    ########
    # ETCD #
    ########
    update_dynamic_settings_stage = DockerBashOperator(task_id='UpdateDynamicSettingsStage',
                                                       dag=dag,
                                                       docker_name='''{{ params.cluster }}''',
                                                       bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et STAGE -p mobile_web'''
    )
    update_dynamic_settings_stage.set_upstream(mobile_web_data_stored)

    if is_prod_env():
        if is_window_dag():
            update_dynamic_settings_prod = DockerBashOperator(task_id='UpdateDynamicSettingsProd',
                                                              dag=dag,
                                                              docker_name='''{{ params.cluster }}''',
                                                              bash_command='''{{ params.execution_dir }}/mobile/scripts/dynamic-settings.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et PRODUCTION -p mobile_web'''
            )

            update_dynamic_settings_prod.set_upstream(copy_to_prod)

    register_success_stage = EtcdSetOperator(task_id='RegisterSuccessOnETCDStage',
                                       dag=dag,
                                       path='''services/mobile-web/moving-window/{{ params.mode }}/{{ ds }}''',
                                       root=ETCD_ENV_ROOT['STAGE']
                                       )
    register_success_stage.set_upstream(mobile_web_data_stored)

    if is_prod_env():

        register_success_prod = EtcdSetOperator(task_id='RegisterSuccessOnETCDProd',
                                           dag=dag,
                                           path='''services/mobile-web/moving-window/{{ params.mode }}/{{ ds }}''',
                                           root=ETCD_ENV_ROOT['PRODUCTION']
        )
        register_success_prod.set_upstream(copy_to_prod)


    return dag


globals()['dag_apps_mw_referrers_moving_window_snapshot'] = generate_dags(SNAPHOT_MODE)
globals()['dag_apps_mw_referrers_moving_window_window'] = generate_dags(WINDOW_MODE)

