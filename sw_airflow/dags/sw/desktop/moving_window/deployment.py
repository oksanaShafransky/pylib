__author__ = 'jeniag'

from sw.airflow.operators import CopyHbaseTableOperator, SuccedOrSkipOperator
from sw.desktop.moving_window.window_calculation import *
from sw.desktop.moving_window.dag import dag_template_params as dag_params

# Copy tables to production

PRODUCTION_ETCD_PREFIX = 'production_mrp'
STAGING_ETCD_PREFIX = 'dev'

copy_to_prod = DummyOperator(task_id='copy_to_prod', dag=temp_dag)
deploy_prod_done = DummyOperator(task_id='deploy_prod_done', dag=temp_dag)
deploy_stage_done = DummyOperator(task_id='deploy_stage_done', dag=temp_dag)
deploy_prod_done.set_upstream(copy_to_prod)
deploy_prod_done.set_upstream(calculation_done)


def prod_switch_function(params):
    if params['deploy_prod']:
        skip_list = []
        success_list = ['can_deploy_prod']
    else:
        skip_list = ['can_deploy_prod']
        success_list = ['deploy_prod_done']
    return skip_list, success_list


can_deploy_prod = SuccedOrSkipOperator(task_id='can_deploy_prod',
                                       dag=temp_dag,
                                       python_callable=prod_switch_function,
                                       op_args=[dag_params])
can_deploy_prod.set_upstream(calculation_done)


def stage_switch_function(params):
    if params['deploy_stage']:
        success_list = ['can_deploy_stage']
        skip_list = []
    else:
        success_list = ['deploy_stage_done']
        skip_list = ['can_deploy_stage']
    return skip_list, success_list


can_deploy_stage = SuccedOrSkipOperator(task_id='can_deploy_stage',
                                        dag=temp_dag,
                                        python_callable=stage_switch_function,
                                        op_args=[dag_params])
can_deploy_stage.set_upstream(calculation_done)



# for target_cluster in ('hbp1', 'hbp2'):
for target_cluster in ('hbp1',):
    copy_to_prod_top_lists = CopyHbaseTableOperator(
        task_id='copy_to_prod_top_lists_%s' % target_cluster,
        dag=temp_dag,
        source_cluster='mrp',
        target_cluster=target_cluster,
        table_name_template="top_lists_last-28_{{ macros.ds_format(ds, '%Y-%m-%d', '%y_%m_%d') }}"
    )
    copy_to_prod_top_lists.set_upstream(ranks)
    copy_to_prod_top_lists.set_upstream(can_deploy_prod)
    copy_to_prod_top_lists.set_downstream(copy_to_prod)

    copy_to_prod_sites_stat = CopyHbaseTableOperator(
        task_id='copy_to_prod_sites_stat_%s' % target_cluster,
        dag=temp_dag,
        source_cluster='mrp',
        target_cluster=target_cluster,
        table_name_template="sites_stat_last-28_{{ macros.ds_format(ds, '%Y-%m-%d', '%y_%m_%d') }}"
    )
    copy_to_prod_sites_stat.set_upstream(calculation_done)
    copy_to_prod_sites_stat.set_downstream(copy_to_prod)
    copy_to_prod_sites_stat.set_upstream(can_deploy_prod)

    copy_to_prod_sites_info = CopyHbaseTableOperator(
        task_id='copy_to_prod_sites_info_%s' % target_cluster,
        dag=temp_dag,
        source_cluster='mrp',
        target_cluster=target_cluster,
        table_name_template="sites_info_last-28_{{ macros.ds_format(ds, '%Y-%m-%d', '%y_%m_%d') }}"
    )
    copy_to_prod_sites_info.set_upstream([misc, ranks])
    copy_to_prod_sites_info.set_downstream(copy_to_prod)
    copy_to_prod_sites_info.set_upstream(can_deploy_prod)


# Dynamic Settings
dynamic_settings_stage = DockerBashOperator(
    task_id='dynamic_settings_stage',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et %s -p update_pro -p update_special_referrers_stage -bd {{ params.base_hdfs_dir }} {{ params.transients }}' % STAGING_ETCD_PREFIX
)

dynamic_settings_stage.set_upstream(can_deploy_stage)
dynamic_settings_stage.set_upstream(calculation_done)
deploy_stage_done.set_upstream(dynamic_settings_stage)

dynamic_settings_prod = DummyOperator(task_id='dynamic_settings_prod', dag=temp_dag)
dynamic_settings_hbp1 = DockerBashOperator(
    task_id='dynamic_settings_hbp1',
    dag=temp_dag,
    docker_name="op-hbp1",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et %s -p update_pro -bd {{ params.base_hdfs_dir }} {{ params.transients }}' % PRODUCTION_ETCD_PREFIX
)

dynamic_settings_hbp1.set_upstream(copy_to_prod)
#
# dynamic_settings_hbp2 = DockerBashOperator(
#     task_id='dynamic_settings_hbp2',
#     dag=temp_dag,
#     docker_name="op-hbp2",
#     bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et production -p update_pro'
# )
#
# dynamic_settings_hbp2.set_upstream(copy_to_prod)

dynamic_settings_sr_prod = DockerBashOperator(
    task_id='dynamic_settings_sr_prod',
    dag=temp_dag,
    docker_name="op-hbp1",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et %s -p update_special_referrers_prod -bd {{ params.base_hdfs_dir }} {{ params.transients }}' % PRODUCTION_ETCD_PREFIX
)

dynamic_settings_sr_prod.set_upstream(copy_to_prod)
dynamic_settings_prod.set_upstream(dynamic_settings_hbp1)
# dynamic_settings_prod.set_upstream(dynamic_settings_hbp2)
dynamic_settings_prod.set_upstream(dynamic_settings_sr_prod)

deploy_prod_done.set_upstream(dynamic_settings_sr_prod)
deploy_prod_done.set_upstream(dynamic_settings_prod)

# Cross cache
calculate_cross_cache = DockerBashOperator(
    task_id='calculate_cross_cache',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/cross-cache.sh -d {{ ds }} -m window -mt last-28 -p create_hive -bd {{ params.base_hdfs_dir }} {{ params.transients }}'
)

calculate_cross_cache.set_upstream(export_rest)

cross_cache_stage = DockerBashOperator(
    task_id='cross_cache_stage',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/cross-cache.sh -d {{ ds }} -m window -mt last-28 -et staging -p update_bucket -bd {{ params.base_hdfs_dir }} {{ params.transients }}'
)

cross_cache_stage.set_upstream(calculate_cross_cache)
cross_cache_stage.set_upstream(dynamic_settings_stage)
deploy_stage_done.set_upstream(cross_cache_stage)

dynamic_settings_cross_stage = DockerBashOperator(
    task_id='dynamic_settings_cross_stage',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et %s -p update_cross_cache -bd {{ params.base_hdfs_dir }} {{ params.transients }}' % STAGING_ETCD_PREFIX
)

dynamic_settings_cross_stage.set_upstream(cross_cache_stage)

cross_cache_prod = DockerBashOperator(
    task_id='cross_cache_prod',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/cross-cache.sh -d {{ ds }} -m window -mt last-28 -et production -p update_bucket -bd {{ params.base_hdfs_dir }} {{ params.transients }}'
)

cross_cache_prod.set_upstream(calculate_cross_cache)
cross_cache_prod.set_upstream(dynamic_settings_prod)

deploy_prod_done.set_upstream(cross_cache_prod)

dynamic_settings_cross_hbp1 = DockerBashOperator(
    task_id='dynamic_settings_cross_hbp1',
    dag=temp_dag,
    docker_name="op-hbp1",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et %s -p update_cross_cache -bd {{ params.base_hdfs_dir }} {{ params.transients }}' % PRODUCTION_ETCD_PREFIX
)

dynamic_settings_cross_hbp1.set_upstream(cross_cache_prod)
dynamic_settings_cross_hbp1.set_upstream(dynamic_settings_prod)

# dynamic_settings_cross_hbp2 = DockerBashOperator(
#     task_id='dynamic_settings_cross_hbp2',
#     dag=temp_dag,
#     docker_name="op-hbp2",
#     bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et production -p update_cross_cache'
# )
#
# dynamic_settings_cross_hbp2.set_upstream(cross_cache_prod)
# dynamic_settings_cross_hbp2.set_upstream(dynamic_settings_prod)
