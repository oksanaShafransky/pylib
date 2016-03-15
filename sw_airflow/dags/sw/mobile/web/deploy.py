from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from sw.airflow.external_sensors import AdaptedExternalTaskSensor
from datetime import timedelta, datetime

from sw.airflow.docker_bash_operator import DockerBashOperatorFactory
from sw.airflow.key_value import KeyValueSetOperator
from sw.airflow.operators import DockerCopyHbaseTableOperator

WINDOW_MODE = 'window'
SNAPSHOT_MODE = 'snapshot'

dag_args = {
    'owner': 'MobileWeb',
    'depends_on_past': False,
    'email': ['amitr@similarweb.com', 'barakg@similarweb.com', 'airflow@similarweb.pagerduty.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2016, 2, 14),
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': '/similargroup/production',
                       'docker_gate': 'docker-a02.sg.internal',
                       'base_data_dir': '/similargroup/data/mobile-analytics',
                       'cluster': 'mrp'
                       }

window_template_params = dag_template_params.copy()
window_template_params.update({'mode': WINDOW_MODE, 'mode_type': 'last-28'})
snapshot_template_params = dag_template_params.copy()
snapshot_template_params.update({'mode': SNAPSHOT_MODE, 'mode_type': 'monthly'})

snapshot_dag = DAG(dag_id='MobileWeb_SnapshotDeploy', default_args=dag_args, params=snapshot_template_params)

window_dag = DAG(dag_id='MobileWeb_WindowDeploy', default_args=dag_args, params=window_template_params,
                 schedule_interval='@daily')


def assemble_process(mode, dag):
    """
    Currently assembles in one dag both finalization of staging process and deployment to production
    :param dag: dag to assemble
    :param mode: use of of constants
    """
    airflow_env = Variable.get(key='airflow_env', default_var='dev')

    full_mobile_web_data_ready = DummyOperator(task_id='full_mobile_web_data_ready', dag=dag)

    mw_dag_id = 'MobileWeb_Window' if mode == WINDOW_MODE else 'MobileWeb_Snapshot'
    mobile_web_data_ready = AdaptedExternalTaskSensor(external_dag_id=mw_dag_id, dag=dag, task_id=mw_dag_id,
                                                      external_task_id=mw_dag_id)
    full_mobile_web_data_ready.set_upstream(mobile_web_data_ready)

    if mode == SNAPSHOT_MODE:
        mobile_web_referrals_data = AdaptedExternalTaskSensor(external_dag_id='MobileWeb_ReferralsSnapshot',
                                                              dag=dag, task_id='MobileWeb_ReferralsSnapshot',
                                                              external_task_id='MobileWeb_ReferralsSnapshot')
        full_mobile_web_data_ready.set_upstream(mobile_web_referrals_data)
        sla = None
    else:
        sla = timedelta(hours=25)

    factory = DockerBashOperatorFactory(use_defaults=True, dag=dag,
                                        script_path='''{{ params.execution_dir }}/mobile/scripts''')

    deploy_targets = Variable.get(key='hbase_deploy_targets', default_var=[], deserialize_json=True)

    hbase_suffix_template = (
        '''{{ params.mode_type }}_{{ macros.ds_format(ds, "%Y-%m-%d", "%y_%m_%d")}}''' if mode == WINDOW_MODE else
        '''{{macros.ds_format(ds, "%Y-%m-%d", "%y_%m")}}''')

    update_dynamic_settings_stage = factory.build(task_id='update_dynamic_settings_stage',
                                                  core_command='dynamic-settings.sh -et staging -p mobile_web')
    update_dynamic_settings_stage.set_upstream(full_mobile_web_data_ready)

    #TODO check if this stage can be deleted
    register_success_stage = \
        KeyValueSetOperator(task_id='register_success_stage',
                            dag=dag,
                            path='''services/mobile-web/moving-window/{{ params.mode }}/{{ ds }}''',
                            env='STAGING')
    register_success_stage.set_upstream(full_mobile_web_data_ready)

    stage_is_set = DummyOperator(task_id='stage_is_set', dag=dag, sla=sla)
    stage_is_set.set_upstream([register_success_stage, update_dynamic_settings_stage])

    if airflow_env == 'prod':
        copy_to_prod = DockerCopyHbaseTableOperator(
                task_id='copy_to_prod',
                dag=dag,
                docker_name='''{{ params.cluster }}''',
                source_cluster='mrp',
                target_cluster=','.join(deploy_targets),
                table_name_template='mobile_web_stats_' + hbase_suffix_template
        )
        copy_to_prod.set_upstream(full_mobile_web_data_ready)

        prod_is_set = DummyOperator(task_id='prod_is_set', dag=dag, sla=sla)

        if mode == WINDOW_MODE:
            update_dynamic_settings_prod = \
                factory.build(task_id='update_dynamic_settings_prod',
                              core_command='dynamic-settings.sh -et PRODUCTION -p mobile_web')
            update_dynamic_settings_prod.set_upstream(copy_to_prod)
            prod_is_set.set_upstream(update_dynamic_settings_prod)

        #TODO check if this stage can be deleted
        register_success_prod = \
            KeyValueSetOperator(task_id='register_success_prod',
                                dag=dag,
                                path='''services/mobile-web/moving-window/{{ params.mode }}/{{ ds }}''',
                                env='PRODUCTION')
        register_success_prod.set_upstream(copy_to_prod)

        prod_is_set.set_upstream(register_success_prod)

    return dag


assemble_process(SNAPSHOT_MODE, snapshot_dag)
assemble_process(WINDOW_MODE, window_dag)
