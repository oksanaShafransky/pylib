import copy

from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta

from sw.airflow.airflow_etcd import *
from sw.airflow.docker_bash_operator import DockerBashOperator, DockerBashOperatorFactory
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
    'email': ['barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2015, 2, 10),
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_data_dir': BASE_DIR, 'run_environment': 'PRODUCTION',
                       'docker_image_name': DEFAULT_CLUSTER, 'hbase_cluster': DEFAULT_HBASE_CLUSTER}

window_template_params = dag_template_params.copy().update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})
snapshot_template_params = dag_template_params.copy().update({'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

dag = DAG(dag_id='MobileWeb_Cleanup', default_args=dag_args, params=window_template_params,
          schedule_interval='@daily')

env = Variable.get(key='env', default_var='dev')

mobile_web_data_stored = DummyOperator(task_id='MobileWebDataStored', dag=dag)

mobile_web_data = ExternalTaskSensor(external_dag_id='MobileWeb_' + mode.capitalize(),
                                     dag=dag, task_id='MobileWeb_' + mode.capitalize(),
                                     external_task_id='MobileWeb_' + mode.capitalize())
mobile_web_data_stored.set_upstream(mobile_web_data)

factory = DockerBashOperatorFactory(use_defaults=True, dag=dag,
                                    date_template='''{{ ds_add(ds,-%s) }}''',
                                    script_path='''{{ params.execution_dir }}/mobile/scripts''')

cleanup_from = 8
cleanup_to = 3

deploy_targets = Variable.get(key='deploy_targets', default_var='{[]}', deserialize_json=True)

cleanup_stage = DummyOperator(task_id='cleanup_stage', dag=dag)
cleanup_prod = DummyOperator(task_id='cleanup_prod', dag=dag)
for i in range(cleanup_to, cleanup_from):
    cleanup_stage_dt_minus_i = \
        factory.build(task_id='cleanup_stage_DT-%s' % i,
                      core_command='''windowCleanup.sh -p delete_files -p drop_hbase_tables -fl mw''' % i)
    cleanup_stage_dt_minus_i.set_upstream(mobile_web_data_stored)
    cleanup_stage.set_upstream(cleanup_stage_dt_minus_i)

    for target in deploy_targets:
        cleanup_day = \
            factory.build(task_id='Cleanup%s_DS-%s' % (target, i),
                          container_name='%s' % target,
                          bash_command='''windowCleanup.sh -d {{ ds_add(ds,-%s) }} -p drop_hbase_tables -fl mw''' % i)
        cleanup_day.set_upstream(copy_to_prod_mw)
        cleanup_prod.set_upstream(cleanup_day)
        cleanup_stage = DummyOperator(task_id='cleanup_stage', dag=dag)
