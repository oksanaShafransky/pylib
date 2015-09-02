__author__ = 'jeniag'

from airflow.operators.dummy_operator import DummyOperator

from sw_airflow.operators import DockerBashOperator, DockerBashSensor
from sw_airflow.desktop.moving_window.dag import window_dag

should_run_window = DockerBashSensor(
    task_id='should_run_window',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='''{{ params.execution_dir }}/analytics/scripts/checks/should_run_window.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }}'''
)


# All daily tasks - aggregation, estimation, and estimation checks
dagg_all = DummyOperator(task_id='dagg_all', dag=window_dag)
dest_all = DummyOperator(task_id='dest_all', dag=window_dag)
dest_check_all = DummyOperator(task_id='dest_check_all', dag=window_dag)
for offset_days in reversed(range(28)):
    daily_aggregation = DockerBashOperator(
        task_id='dagg_%d' % offset_days,
        dag=window_dag,
        docker_name="op-hbs2",
        bash_command='{{ params.execution_dir }}/analytics/scripts/daily/dailyAggregation.sh -d {{ macros.ds_add(ds, -%d) }} -m window -mt last-28 {{transients}}' % offset_days
    )

    daily_estimation = DockerBashOperator(
        task_id='dest_%d' % offset_days,
        dag=window_dag,
        docker_name="op-hbs2",
        bash_command='{{ params.execution_dir }}/analytics/scripts/daily/dailyEstimation.sh -d {{ macros.ds_add(ds, -%d) }} -m window -mt last-28 --outliersdate {{ ds }} {{transients}}' % offset_days
    )

    daily_estimation_check = DockerBashOperator(
        task_id='dest_check_%d' % offset_days,
        dag=window_dag,
        docker_name="op-hbs2",
        bash_command='{{ params.execution_dir }}/analytics/scripts/daily/qa/checkSiteAndCountryEstimation.sh -d {{ macros.ds_add(ds, -%d) }} -m window -mt last-28 -nw 7"' % offset_days
    )

    daily_aggregation.set_upstream(should_run_window)
    daily_estimation.set_upstream(daily_aggregation)
    daily_estimation_check.set_upstream(daily_estimation)

    dagg_all.set_upstream(daily_aggregation)
    dest_all.set_upstream(daily_estimation)
    dest_check_all.set_upstream(daily_estimation)
