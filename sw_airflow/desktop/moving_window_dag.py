# Test for deciding when to start
from copy import deepcopy

__author__ = 'jeniag'

from sw_airflow.desktop.moving_window.daily_calculation import *
from sw_airflow.desktop.moving_window.window_calculation import *
from sw_airflow.desktop.moving_window.deployment import *
from sw_airflow.desktop.moving_window.post_deployment import *
from sw_airflow.desktop.moving_window.dag import *

window_dag = DAG(dag_id='moving_window', default_args=dag_args, params=dag_template_params)
window_dag.add_tasks(temp_dag.tasks)
