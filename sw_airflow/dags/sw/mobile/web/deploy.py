from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta

from sw.airflow.airflow_etcd import *
from sw.airflow.docker_bash_operator import DockerBashOperatorFactory
from sw.airflow.operators import DockerCopyHbaseTableOperator

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
    'email': ['amitr@similarweb.com', 'barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2015, 2, 10),
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_data_dir': BASE_DIR, 'run_environment': 'PRODUCTION',
                       'cluster': DEFAULT_CLUSTER, 'hbase_cluster': DEFAULT_HBASE_CLUSTER}

window_template_params = dag_template_params.copy().update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})
snapshot_template_params = dag_template_params.copy().update({'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

snapshot_dag = DAG(dag_id='MobileWeb_SnapshotDeploy', default_args=dag_args, params=snapshot_template_params,
                   schedule_interval='@monthly')

window_dag = DAG(dag_id='MobileWeb_WindowDeploy', default_args=dag_args, params=window_template_params,
                 schedule_interval='@daily')


def assemble_process(mode, dag):
    def is_window_dag():
        return mode == WINDOW_MODE

    env = Variable.get(key='env', default_var='dev')

    full_mobile_web_data_ready = DummyOperator(task_id='full_mobile_web_data_ready', dag=dag)

    mobile_web_data_ready = ExternalTaskSensor(external_dag_id='MobileWeb_' + mode.capitalize(),
                                               dag=dag, task_id='MobileWeb_' + mode.capitalize(),
                                               external_task_id='MobileWeb_' + mode.capitalize())
    full_mobile_web_data_ready.set_upstream(mobile_web_data_ready)

    if mode == SNAPHOT_MODE:
        mobile_web_referrals_data = ExternalTaskSensor(external_dag_id='MobileWeb_Referrals' + mode.capitalize(),
                                                       dag=dag, task_id='MobileWeb_Referrals' + mode.capitalize(),
                                                       external_task_id='MobileWeb_Referrals' + mode.capitalize())
        full_mobile_web_data_ready.set_upstream(mobile_web_referrals_data)

    factory = DockerBashOperatorFactory(use_defaults=True, dag=dag,
                                        script_path='''{{ params.execution_dir }}/mobile/scripts''')

    deploy_targets = Variable.get(key='deploy_targets', default_var='{[]}', deserialize_json=True)

    hbase_suffix_template = (
        '''{{ params.mode_type }}_{{ macros.ds_format(ds, "%Y-%m-%d", "%y_%m_%d")}}''' if is_window_dag() else
        '''{{macros.ds_format(ds, "%Y-%m-%d", "%y_%m")}}''')

    copy_to_prod = DockerCopyHbaseTableOperator(
            task_id='copy_to_prod',
            dag=dag,
            docker_name='''{{ params.cluster }}''',
            source_cluster='mrp',
            target_cluster=','.join(deploy_targets),
            table_name_template='mobile_web_stats_' + hbase_suffix_template
    )
    copy_to_prod.set_upstream(full_mobile_web_data_ready)

    update_dynamic_settings_stage = factory.build(task_id='UpdateDynamicSettingsStage',
                                                  core_command='''dynamic-settings.sh -et STAGE -p mobile_web''')
    update_dynamic_settings_stage.set_upstream(full_mobile_web_data_ready)

    if mode == WINDOW_MODE:
        update_dynamic_settings_prod = factory.build(task_id='update_dynamic_settings_prod',
                                                     core_command='dynamic-settings.sh -et PRODUCTION -p mobile_web')
        update_dynamic_settings_prod.set_upstream(copy_to_prod)

    register_success_stage = EtcdSetOperator(task_id='register_success_stage',
                                             dag=dag,
                                             path='''services/mobile-web/moving-window/{{ params.mode }}/{{ ds }}''',
                                             root=ETCD_ENV_ROOT['STAGE'])
    register_success_stage.set_upstream(full_mobile_web_data_ready)

    register_success_prod = EtcdSetOperator(task_id='register_success_prod',
                                            dag=dag,
                                            path='''services/mobile-web/moving-window/{{ params.mode }}/{{ ds }}''',
                                            root=ETCD_ENV_ROOT['PRODUCTION'])
    register_success_prod.set_upstream(copy_to_prod)
    return dag


assemble_process(SNAPHOT_MODE, snapshot_dag)
assemble_process(WINDOW_MODE, window_dag)
