from airflow import DAG

from sw.desktop.moving_window.dag import *

__author__ = 'jeniag'

window_dag = DAG(dag_id='moving_window', default_args=dag_args, params=dag_template_params)
window_dag.add_tasks(temp_dag.tasks)
