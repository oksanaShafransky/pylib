__author__ = 'Felix Vaisman'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.sensors import HdfsSensor

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(15, 11, 13),
    'depends_on_past': False,
    'email': ['bigdata@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='DesktopDailyPanelReport', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


# define stages

group_files_base = '/similargroup/data/stats'
group_files_ready = HdfsSensor(task_id='GroupFilesReady',
                               dag=dag,
                               hdfs_conn_id='hdfs_%s' % DEFAULT_CLUSTER,
                               filepath='''%s/{{ macros.date_partition(ds) }}/_SUCCESS''' % group_files_base,
                               execution_timeout=timedelta(minutes=240)
                               )


blocked_ips_base = '/similargroup/data/analytics/daily/blocked_ips'
blocked_ips_ready = HdfsSensor(task_id='BlockedIpsReady',
                               dag=dag,
                               hdfs_conn_id='hdfs_%s' % DEFAULT_CLUSTER,
                               filepath='''%s/{{ macros.date_partition(ds) }}/_SUCCESS''' % blocked_ips_base,
                               execution_timeout=timedelta(minutes=240)
                               )

panel_report = DockerBashOperator(task_id='PanelStats',
                                  dag=dag,
                                  docker_name='''{{ params.cluster }}''',
                                  bash_command='''{{ params.execution_dir }}/analytics/scripts/panel/panelReports.sh -d {{ ds }}'''
                                  )
panel_report.set_upstream(group_files_ready)
panel_report.set_upstream(blocked_ips_ready)



