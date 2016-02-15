__author__ = 'Kfir Eittan'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.docker_bash_operator import DockerBashOperator
from sw.airflow.external_sensors import AdaptedExternalTaskSensor

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
WINDOW_MODE = 'window'
SNAPHOT_MODE = 'snapshot'
WINDOW_MODE_TYPE = 'last-28'
SNAPSHOT_MODE_TYPE = 'monthly'
DEFAULT_HBASE_CLUSTER = 'hbp1'
DEPLOY_TARGETS = Variable.get("hbase_deploy_targets", deserialize_json=True)['hbp1', 'hbp2']

dag_args = {
    'owner': 'similarweb',
    'depends_on_past': False,
    'email': ['kfire@similarweb.com','amitr@similarweb.com','andrews@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2016, 2, 11)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION',
                       'cluster': DEFAULT_CLUSTER, 'hbase_cluster': DEFAULT_HBASE_CLUSTER,
                       'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE}

dag = DAG(dag_id='Desktop_MovingWindow_Cleanup', default_args=dag_args, params=dag_template_params, schedule_interval="@daily")

moving_window = AdaptedExternalTaskSensor(external_dag_id='Desktop_MovingWindow_Window',
                                       external_task_id='NonOperationals',
                                       task_id='MovingWindow',
                                       dag=dag)

cleanup_from_days = 8
cleanup_to_days = 4

cleanup_stage = DummyOperator(task_id='CleanupStage',
                              dag=dag)

for i in range(cleanup_to_days, cleanup_from_days):
    cleanup_stage_ds_minus_i = \
        DockerBashOperator(task_id='CleanupStage_DS-%s' % i,
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/windowCleanup.sh -d {{ macros.ds_add(macros.last_interval_day(ds, dag.schedule_interval),-%s) }} -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et staging -p delete_files,drop_hbase_tables''' % i
                           )
    cleanup_stage_ds_minus_i.set_upstream(cleanup_stage)

cleanup_stage.set_upstream(moving_window)

cleanup_from_days = 12
cleanup_to_days = 5

cleanup_prod = DummyOperator(task_id='CleanupProd',
                             dag=dag)

for i in range(cleanup_to_days, cleanup_from_days):
    cleanup_prod_ds_minus_i = DummyOperator(task_id='CleanupProd_DS-%s' % i,
                                            dag=dag)
    cleanup_prod_ds_minus_i.set_upstream(cleanup_prod)

    for target in DEPLOY_TARGETS:
        cleanup_prod_per_target_ds_minus_i = \
            DockerBashOperator(task_id='CleanupProd_%s_DS-%s' % (target, i),
                               dag=dag,
                               docker_name='%s' % target,
                               bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/windowCleanup.sh -d {{ macros.ds_add(macros.last_interval_day(ds, dag.schedule_interval),-%s) }}  -bd {{ params.base_hdfs_dir }} -m {{ params.mode }} -mt {{ params.mode_type }} -et production -p drop_hbase_tables''' % i
                               )
        cleanup_prod_per_target_ds_minus_i.set_upstream(cleanup_prod_ds_minus_i)

cleanup_prod.set_upstream(moving_window)
