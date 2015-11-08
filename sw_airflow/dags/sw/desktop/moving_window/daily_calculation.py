__author__ = 'jeniag'

from airflow.operators.dummy_operator import DummyOperator

from sw.common.operators import DockerBashOperator, DockerBashSensor
from sw.desktop.moving_window.dag import temp_dag

should_run_window = DockerBashSensor(
    task_id='should_run_window',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='''{{ params.execution_dir }}/analytics/scripts/checks/should_run_window.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }} {{ params.transients }}'''
)


# All daily tasks - aggregation, estimation, and estimation checks
dagg_all = DummyOperator(task_id='dagg_all', dag=temp_dag)
dest_all = DummyOperator(task_id='dest_all', dag=temp_dag)
dest_check_all = DummyOperator(task_id='dest_check_all', dag=temp_dag)
for offset_days in reversed(range(28)):
    daily_aggregation = DockerBashOperator(
        task_id='dagg_%d' % offset_days,
        dag=temp_dag,
        docker_name="{{ params.default_docker }}",
        bash_command='{{ params.execution_dir }}/analytics/scripts/daily/dailyAggregation.sh -d {{ macros.ds_add(ds, -%d) }} -bd {{ params.base_hdfs_dir }} {{ params.transients }}' % offset_days
    )

    daily_estimation = DockerBashOperator(
        task_id='dest_%d' % offset_days,
        dag=temp_dag,
        docker_name="{{ params.default_docker }}",
        bash_command='{{ params.execution_dir }}/analytics/scripts/daily/dailyEstimation.sh -d {{ macros.ds_add(ds, -%d) }} --outliersdate {{ ds }} -bd {{ params.base_hdfs_dir }} {{ params.transients }}' % offset_days
    )

    daily_estimation_check = DockerBashOperator(
        task_id='dest_check_%d' % offset_days,
        dag=temp_dag,
        docker_name="{{ params.default_docker }}",
        bash_command='{{ params.execution_dir }}/analytics/scripts/daily/qa/checkSiteAndCountryEstimation.sh -d {{ macros.ds_add(ds, -%d) }} -m window -mt last-28 -nw 7 -bd {{ params.base_hdfs_dir }} {{ params.transients }}' % offset_days
    )

    daily_aggregation.set_upstream(should_run_window)
    daily_estimation.set_upstream(daily_aggregation)
    daily_estimation_check.set_upstream(daily_estimation)

    dagg_all.set_upstream(daily_aggregation)
    dest_all.set_upstream(daily_estimation)
    dest_check_all.set_upstream(daily_estimation)
