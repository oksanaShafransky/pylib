__author__ = 'jeniag'

from airflow.operators.dummy_operator import DummyOperator
from sw_airflow.operators import DockerBashOperator, CopyHbaseTableOperator
from sw_airflow.desktop.moving_window.window_calculation import *

# Copy tables to production
copy_to_prod = DummyOperator(task_id='copy_to_prod', dag=window_dag)
for target_cluster in ('hbp1', 'hbp2'):
    copy_to_prod_top_lists = CopyHbaseTableOperator(
        task_id='copy_to_prod_top_lists_%s' % target_cluster,
        dag=window_dag,
        source_cluster='hbs2',
        target_cluster=target_cluster,
        table_name_template="top_lists_last-28_{{ macros.ds_format(ds, '%Y-%m-%d', '%y_%m_%d') }}"
    )
    copy_to_prod_top_lists.set_upstream(ranks)
    copy_to_prod_top_lists.set_downstream(copy_to_prod)

    copy_to_prod_sites_stat = CopyHbaseTableOperator(
        task_id='copy_to_prod_sites_stat_%s' % target_cluster,
        dag=window_dag,
        source_cluster='hbs2',
        target_cluster=target_cluster,
        table_name_template="sites_stat_last-28_{{ macros.ds_format(ds, '%Y-%m-%d', '%y_%m_%d') }}"
    )
    copy_to_prod_sites_stat.set_upstream(all_calculation)
    copy_to_prod_sites_stat.set_downstream(copy_to_prod)

    copy_to_prod_sites_info = CopyHbaseTableOperator(
        task_id='copy_to_prod_sites_info_%s' % target_cluster,
        dag=window_dag,
        source_cluster='hbs2',
        target_cluster=target_cluster,
        table_name_template="sites_info_last-28_{{ macros.ds_format(ds, '%Y-%m-%d', '%y_%m_%d') }}"
    )
    copy_to_prod_sites_info.set_upstream([misc, ranks])
    copy_to_prod_sites_info.set_downstream(copy_to_prod)


# Dynamic Settings
dynamic_settings_stage = DockerBashOperator(
    task_id='dynamic_settings_stage',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et staging -p update_pro -p update_special_referrers_stage'
)

dynamic_settings_stage.set_upstream(all_calculation)

dynamic_settings_prod = DummyOperator(task_id='dynamic_settings_prod', dag=window_dag)
dynamic_settings_hbp1 = DockerBashOperator(
    task_id='dynamic_settings_hbp1',
    dag=window_dag,
    docker_name="op-hbp1",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et production -p update_pro'
)

dynamic_settings_hbp1.set_upstream(copy_to_prod)

dynamic_settings_hbp2 = DockerBashOperator(
    task_id='dynamic_settings_hbp2',
    dag=window_dag,
    docker_name="op-hbp2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et production -p update_pro'
)

dynamic_settings_hbp2.set_upstream(copy_to_prod)

dynamic_settings_sr_prod = DockerBashOperator(
    task_id='dynamic_settings_sr_prod',
    dag=window_dag,
    docker_name="op-hbp1",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et production -p update_special_referrers_prod'
)

dynamic_settings_sr_prod.set_upstream(copy_to_prod)
dynamic_settings_prod.set_upstream(dynamic_settings_hbp1)
dynamic_settings_prod.set_upstream(dynamic_settings_hbp2)
dynamic_settings_prod.set_upstream(dynamic_settings_sr_prod)

# Cross cache
calculate_cross_cache = DockerBashOperator(
    task_id='calculate_cross_cache',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/cross-cache.sh -d {{ ds }} -m window -mt last-28 -p create_hive'
)

calculate_cross_cache.set_upstream(export_rest)

cross_cache_stage = DockerBashOperator(
    task_id='cross_cache_stage',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/cross-cache.sh -d {{ ds }} -m window -mt last-28 -et staging -p update_bucket'
)

cross_cache_stage.set_upstream(calculate_cross_cache)
cross_cache_stage.set_upstream(dynamic_settings_stage)

dynamic_settings_cross_stage = DockerBashOperator(
    task_id='dynamic_settings_cross_stage',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et staging -p update_cross_cache'
)

dynamic_settings_cross_stage.set_upstream(cross_cache_stage)

cross_cache_prod = DockerBashOperator(
    task_id='cross_cache_prod',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/cross-cache.sh -d {{ ds }} -m window -mt last-28 -et staging -p update_bucket'
)

cross_cache_prod.set_upstream(calculate_cross_cache)
cross_cache_prod.set_upstream(dynamic_settings_prod)

dynamic_settings_cross_hbp1 = DockerBashOperator(
    task_id='dynamic_settings_cross_hbp1',
    dag=window_dag,
    docker_name="op-hbp1",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et production -p update_cross_cache'
)

dynamic_settings_cross_hbp1.set_upstream(cross_cache_prod)
dynamic_settings_cross_hbp1.set_upstream(dynamic_settings_prod)

dynamic_settings_cross_hbp2 = DockerBashOperator(
    task_id='dynamic_settings_cross_hbp2',
    dag=window_dag,
    docker_name="op-hbp2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/dynamic-settings.sh -d {{ ds }} -m window -mt last-28 -et production -p update_cross_cache'
)

dynamic_settings_cross_hbp2.set_upstream(cross_cache_prod)
dynamic_settings_cross_hbp2.set_upstream(dynamic_settings_prod)


# TODO: Move cleanup and hive repair to a different DAG/module

# Cleanup
cleanup_all = DummyOperator(task_id="cleanup_all", dag=window_dag)
cleanup_stage_all = DummyOperator(task_id="cleanup_stage_all", dag=window_dag)
cleanup_prod_all = DummyOperator(task_id="cleanup_prod_all", dag=window_dag)
cleanup_all.set_upstream([cleanup_stage_all, cleanup_prod_all])
for offset_days in reversed(range(7, 13)):
    cleanup_stage = DockerBashOperator(
        task_id='cleanup_stage_%d' % offset_days,
        dag=window_dag,
        docker_name="op-hbs2",
        bash_command='{{ params.execution_dir }}/$DIR/daily/windowCleanup.sh -d {{ macros.ds_add(ds, -%d) }} -m window -mt last-28 -et staging -p delete_files,drop_crosscache_stage,drop_hbase_tables' % offset_days
    )
    cleanup_prod = DummyOperator(task_id='cleanup_prod_%d' % offset_days, dag=window_dag)
    for docker in ('op-hbp1', 'op-hbp2'):
        cleanup_prod_single = DockerBashOperator(
            task_id='cleanup_prod_%d_%s' % (offset_days, docker),
            dag=window_dag,
            docker_name=docker,
            bash_command='{{ params.execution_dir }}/$DIR/daily/windowCleanup.sh -d {{ macros.ds_add(ds, -%d) }} -m window -mt last-28 -et production -p drop_crosscache_stage,drop_hbase_tables' % offset_days
        )
        cleanup_prod.set_upstream(cleanup_prod_single)
        cleanup_prod_single.set_upstream(cross_cache_prod)
        cleanup_prod_single.set_upstream(dynamic_settings_prod)

    cleanup_stage.set_upstream(cross_cache_stage)
    cleanup_stage.set_upstream(dynamic_settings_stage)
    cleanup_stage_all.set_upstream(cleanup_stage)
    cleanup_prod_all.set_upstream(cleanup_prod)


# Repair hive tables

repair_hive_tables = DummyOperator(task_id='repair_hive_tables', dag=window_dag)

repair_dest_tables = DockerBashOperator(
    task_id='repair_dest_tables',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/$DIR/daily/dailyEstimation.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_dest_tables.set_upstream(dest_all)

repair_dagg_tables = DockerBashOperator(
    task_id='repair_dagg_tables',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/$DIR/daily/dailyAggregation.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_dagg_tables.set_upstream(dagg_all)

repair_incoming_tables = DockerBashOperator(
    task_id='repair_incoming_tables',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_incoming_tables.set_upstream(incoming)

repair_outgoing_tables = DockerBashOperator(
    task_id='repair_outgoing_tables',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_outgoing_tables.set_upstream(outgoing)

repair_keywords_tables = DockerBashOperator(
    task_id='repair_keywords_tables',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_keywords_tables.set_upstream(keywords)

repair_ranks_tables = DockerBashOperator(
    task_id='repair_ranks_tables',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_ranks_tables.set_upstream(ranks)
repair_ranks_tables.set_upstream(export_rest)

repair_sr_tables = DockerBashOperator(
    task_id='repair_sr_tables',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_sr_tables.set_upstream(traffic_distro)

repair_sending_pages_tables = DockerBashOperator(
    task_id='repair_sending_pages_tables',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/sending-pages.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_sending_pages_tables.set_upstream(sending_pages)

repair_popular_pages_tables = DockerBashOperator(
    task_id='repair_popular_pages_tables',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/popular-pages.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_popular_pages_tables.set_upstream(popular_pages)

repair_social_receiving_tables = DockerBashOperator(
    task_id='repair_social_receiving_tables',
    dag=window_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/social-receiving.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_social_receiving_tables.set_upstream(social_receiving)
repair_hive_tables.set_upstream([repair_dagg_tables, repair_dest_tables, repair_incoming_tables, repair_keywords_tables,
                                 repair_outgoing_tables, repair_popular_pages_tables, repair_ranks_tables,
                                 repair_sending_pages_tables, repair_social_receiving_tables, repair_sr_tables])

