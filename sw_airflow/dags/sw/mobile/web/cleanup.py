from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta

from sw.airflow.airflow_etcd import *
from sw.airflow.docker_bash_operator import DockerBashOperatorFactory

dag_args = {
    'owner': 'similarweb',
    'depends_on_past': False,
    'email': ['barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2016, 2, 10),
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': '/similargroup/production',
                       'docker_gate': 'docker-a02.sg.internal',
                       'base_data_dir': '/similargroup/data/mobile-analytics',
                       'run_environment': 'PRODUCTION',
                       'docker_image_name': 'mrp-mrp',
                       'mode': 'window',
                       'mode_type': 'last-28'
                       }

dag = DAG(dag_id='MobileWeb_Cleanup', default_args=dag_args, params=dag_template_params, schedule_interval='@daily')

factory = DockerBashOperatorFactory(use_defaults=True,
                                    dag=dag,
                                    script_path='''{{ params.execution_dir }}/mobile/scripts''')

cleanup_from = 8
cleanup_to = 3

deploy_targets = Variable.get(key='deploy_targets', default_var=[], deserialize_json=True)
airflow_env = Variable.get(key='airflow_env', default_var='dev')

stage_is_set = ExternalTaskSensor(external_dag_id='MobileWeb_WindowDeploy', dag=dag, task_id='window_in_stage_is_set',
                                  external_task_id='stage_is_set')
cleanup_stage = DummyOperator(task_id='cleanup_stage', dag=dag)
for day_to_clean in range(cleanup_to, cleanup_from):
    cleanup_day = \
        factory.build(task_id='cleanup_stage_DT_%s' % day_to_clean,
                      date_template='''{{ macros.ds_add(ds,-%d) }}''' % day_to_clean,
                      core_command='windowCleanup.sh -p delete_files -p drop_hbase_tables -fl mw')
    cleanup_day.set_upstream(stage_is_set)
    cleanup_stage.set_upstream(cleanup_day)

if airflow_env == 'prod':
    prod_is_set = ExternalTaskSensor(external_dag_id='MobileWeb_WindowDeploy', dag=dag, task_id='window_in_prod_is_set',
                                     external_task_id='prod_is_set')
    for target in deploy_targets:
        cleanup_prod = DummyOperator(task_id='cleanup_prod_%s' % target, dag=dag)
        for day_to_clean in range(cleanup_to, cleanup_from):
            cleanup_day = \
                factory.build(task_id='cleanup_prod_%s_%s' % (target, day_to_clean),
                              docker_image_name='%s' % target,
                              date_template='''{{ macros.ds_add(ds,-%d) }}''' % day_to_clean,
                              core_command='windowCleanup.sh -p drop_hbase_tables -fl mw -dock %s' % target)
            cleanup_day.set_upstream(prod_is_set)
            cleanup_prod.set_upstream(cleanup_day)
