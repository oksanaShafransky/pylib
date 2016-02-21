from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from sw.airflow.external_sensors import AdaptedExternalTaskSensor
from datetime import timedelta, datetime

from sw.airflow.docker_bash_operator import DockerBashOperatorFactory

dag_args = {
    'owner': 'MobileWeb',
    'depends_on_past': False,
    'email': ['barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2016, 2, 15),
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': '/similargroup/production',
                       'docker_gate': 'docker-a02.sg.internal',
                       'base_data_dir': '/similargroup/data/mobile-analytics',
                       'run_environment': 'PRODUCTION',
                       'cluster': 'mrp',
                       'mode': 'window',
                       'mode_type': 'last-28'
                       }

dag = DAG(dag_id='MobileWeb_Cleanup', default_args=dag_args, params=dag_template_params, schedule_interval='@daily')

factory = DockerBashOperatorFactory(use_defaults=True,
                                    dag=dag,
                                    script_path='''{{ params.execution_dir }}/mobile/scripts''')

stage_cleanup_from = 8
stage_cleanup_to = 4

deploy_targets = Variable.get(key='hbase_deploy_targets', default_var=[], deserialize_json=True)
airflow_env = Variable.get(key='airflow_env', default_var='dev')

stage_is_set = AdaptedExternalTaskSensor(external_dag_id='MobileWeb_WindowDeploy', dag=dag,
                                         task_id='window_in_stage_is_set',
                                         external_task_id='stage_is_set')
cleanup_stage = DummyOperator(task_id='cleanup_stage', dag=dag)
for day_to_clean in range(stage_cleanup_to, stage_cleanup_from):
    cleanup_day = \
        factory.build(task_id='cleanup_stage_DT_%s' % day_to_clean,
                      date_template='''{{ macros.ds_add(ds,-%d) }}''' % day_to_clean,
                      core_command='windowCleanup.sh -p delete_files -p drop_hbase_tables -fl MOBILE_WEB -et staging')
    cleanup_day.set_upstream(stage_is_set)
    cleanup_stage.set_upstream(cleanup_day)

prod_cleanup_from = 12
prod_cleanup_to = 5
if airflow_env == 'prod':
    prod_is_set = AdaptedExternalTaskSensor(external_dag_id='MobileWeb_WindowDeploy', dag=dag,
                                            task_id='window_in_prod_is_set',
                                            external_task_id='prod_is_set')
    for target in deploy_targets:
        cleanup_prod = DummyOperator(task_id='cleanup_prod_%s' % target, dag=dag)
        for day_to_clean in range(prod_cleanup_to, prod_cleanup_from):
            cleanup_day = \
                factory.build(task_id='cleanup_prod_%s_%s' % (target, day_to_clean),
                              cluster=target,
                              date_template='''{{ macros.ds_add(ds,-%d) }}''' % day_to_clean,
                              core_command='windowCleanup.sh -p drop_hbase_tables -fl MOBILE_WEB -et production')
            cleanup_day.set_upstream(prod_is_set)
            cleanup_prod.set_upstream(cleanup_day)
