MASTER_EXECUTION_DIR = '/home/jeniag/similargroup_master'
CDH5_EXECUTION_DIR = '/home/jeniag/similargroup_cdh5_test'
__author__ = 'jeniag'

from datetime import datetime, timedelta

from airflow.models import DAG

from sw.common.operators import DockerBashSensor, DockerBashOperator

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2015, 7, 29),
    'schedule_interval': timedelta(minutes=5),
}

dag_template_params = {'execution_dir': MASTER_EXECUTION_DIR, 'docker_gate': 'docker-a01.sg.internal',
                       'base_hdfs_dir': '/similargroup/data/analytics'}
dag = DAG(dag_id='window_orchestration', default_args=dag_args, params=dag_template_params)

should_run_preliminary = DockerBashSensor(
    task_id='should_run_preliminary',
    dag=dag,
    docker_name="mrp",
    params={'execution_dir': CDH5_EXECUTION_DIR},
    bash_command='''{{ params.execution_dir }}/analytics/scripts/checks/should_run_preleminary.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }}'''
)

should_run_panel_reports = DockerBashSensor(
    task_id='should_run_panel_reports',
    dag=dag,
    docker_name="mrp",
    params={'execution_dir': CDH5_EXECUTION_DIR},
    bash_command='''{{ params.execution_dir }}/analytics/scripts/checks/should_run_simple_panel_reports.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }}'''
)

should_copy_stats = DockerBashSensor(
    task_id='should_copy_stats',
    dag=dag,
    docker_name="mrp",
    params={'execution_dir': CDH5_EXECUTION_DIR},
    bash_command='{{ params.execution_dir }}/analytics/scripts/checks/should_copy_stats.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }}'
)

should_run_window = DockerBashSensor(
    task_id='should_run_window',
    dag=dag,
    docker_name="{{ params.default_docker }}",
    bash_command='''{{ params.execution_dir }}/analytics/scripts/checks/should_run_window.sh -d {{ ds }} -bd {{ params.base_hdfs_dir }}'''
)

run_preliminary = DockerBashOperator(task_id='preliminary_jobs',
                                     dag=dag,
                                     docker_name='mrp',
                                     params={'execution_dir': CDH5_EXECUTION_DIR},
                                     bash_command='''{{ params.execution_dir }}/analytics/scripts/daily/preliminaryJobs.sh -s {{ ds }} -e {{ ds }} -p group,blocked_ips --dryrun'''
                                     )

panel_reports_cmd = '''{{ params.execution_dir }}/analytics/scripts/panel/panelReports.sh -s {{ ds }} -e {{ ds }} \
                                                     -p raw,stat_report,merge_users_table,activity_report,joined_report,stats_to_sql,alerts_to_sql,churn_report,mail \
                                                     -sm --dryrun'''
run_panel_reports = DockerBashOperator(task_id='panel_reports',
                                       dag=dag,
                                       docker_name='mrp',
                                       params={'execution_dir': CDH5_EXECUTION_DIR},
                                       bash_command=panel_reports_cmd)

run_copy_stats = DockerBashOperator(task_id='copy_stats',
                                    dag=dag,
                                    docker_name='mrp',
                                    params={'execution_dir': CDH5_EXECUTION_DIR},
                                    bash_command='''{{ params.execution_dir }}/analytics/scripts/copy-tasks.sh -s {{ ds }} -e {{ ds }} --dryrun'''
                                    )

run_window = DockerBashOperator(task_id='moving_window',
                                dag=dag,
                                docker_name='op-hbs2',
                                bash_command='''{{ params.execution_dir }}/analytics/scripts/moving_window.sh --rundeck -m window -mt last-28 -d {{ ds}} -bd {{ params.base_hdfs_dir }} --dryrun''')

run_preliminary.set_upstream(should_run_preliminary)
run_panel_reports.set_upstream(should_run_panel_reports)
run_panel_reports.set_upstream(run_preliminary)
run_copy_stats.set_upstream(should_copy_stats)
run_copy_stats.set_upstream(run_preliminary)
run_window.set_upstream(should_run_window)
run_window.set_upstream(run_copy_stats)
