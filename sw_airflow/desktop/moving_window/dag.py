__author__ = 'jeniag'

MASTER_EXECUTION_DIR = '/home/jeniag/similargroup_master'
CDH5_EXECUTION_DIR = '/home/jeniag/similargroup_cdh5_test'
__author__ = 'jeniag'

from airflow.models import DAG
from datetime import datetime, timedelta

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2015, 7, 29),
    'schedule_interval': timedelta(minutes=5),
}

dag_template_params = {'execution_dir': MASTER_EXECUTION_DIR, 'docker_gate': 'docker-a01.sg.internal',
                       'base_hdfs_dir': '/similargroup/data/analytics',
                       'transients': '',
                       'deploy_prod': False,
                       'deploy_stage': True}

temp_dag = DAG(dag_id='moving_window', default_args=dag_args, params=dag_template_params)
