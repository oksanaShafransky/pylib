__author__ = 'jeniag'

from sw_airflow.desktop.moving_window.deployment import *

# Cleanup
cleanup_all = DummyOperator(task_id="cleanup_all", dag=temp_dag)
cleanup_stage_all = DummyOperator(task_id="cleanup_stage_all", dag=temp_dag)
cleanup_prod_all = DummyOperator(task_id="cleanup_prod_all", dag=temp_dag)
cleanup_all.set_upstream([cleanup_stage_all, cleanup_prod_all])
for offset_days in reversed(range(7, 13)):
    cleanup_stage = DockerBashOperator(
        task_id='cleanup_stage_%d' % offset_days,
        dag=temp_dag,
        docker_name="op-hbs2",
        bash_command='{{ params.execution_dir }}/analytics/scripts/daily/windowCleanup.sh -d {{ macros.ds_add(ds, -%d) }} -m window -mt last-28 -et staging -p delete_files,drop_crosscache_stage,drop_hbase_tables' % offset_days
    )
    cleanup_prod = DummyOperator(task_id='cleanup_prod_%d' % offset_days, dag=temp_dag)
    for docker in ('op-hbp1', 'op-hbp2'):
        cleanup_prod_single = DockerBashOperator(
            task_id='cleanup_prod_%d_%s' % (offset_days, docker),
            dag=temp_dag,
            docker_name=docker,
            bash_command='{{ params.execution_dir }}/analytics/scripts/daily/windowCleanup.sh -d {{ macros.ds_add(ds, -%d) }} -m window -mt last-28 -et production -p drop_crosscache_stage,drop_hbase_tables' % offset_days
        )
        cleanup_prod.set_upstream(cleanup_prod_single)
        cleanup_prod_single.set_upstream(deploy_prod_done)

    cleanup_stage.set_upstream(deploy_stage_done)
    cleanup_stage_all.set_upstream(cleanup_stage)
    cleanup_prod_all.set_upstream(cleanup_prod)


# Repair hive tables

repair_hive_tables = DummyOperator(task_id='repair_hive_tables', dag=temp_dag)

repair_dest_tables = DockerBashOperator(
    task_id='repair_dest_tables',
    dag=temp_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/daily/dailyEstimation.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_dest_tables.set_upstream(dest_all)

repair_dagg_tables = DockerBashOperator(
    task_id='repair_dagg_tables',
    dag=temp_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/daily/dailyAggregation.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_dagg_tables.set_upstream(dagg_all)

repair_incoming_tables = DockerBashOperator(
    task_id='repair_incoming_tables',
    dag=temp_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_incoming_tables.set_upstream(incoming)

repair_outgoing_tables = DockerBashOperator(
    task_id='repair_outgoing_tables',
    dag=temp_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_outgoing_tables.set_upstream(outgoing)

repair_keywords_tables = DockerBashOperator(
    task_id='repair_keywords_tables',
    dag=temp_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_keywords_tables.set_upstream(keywords)

repair_ranks_tables = DockerBashOperator(
    task_id='repair_ranks_tables',
    dag=temp_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_ranks_tables.set_upstream(ranks)
repair_ranks_tables.set_upstream(export_rest)

repair_sr_tables = DockerBashOperator(
    task_id='repair_sr_tables',
    dag=temp_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_sr_tables.set_upstream(traffic_distro)

repair_sending_pages_tables = DockerBashOperator(
    task_id='repair_sending_pages_tables',
    dag=temp_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/sending-pages.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_sending_pages_tables.set_upstream(sending_pages)

repair_popular_pages_tables = DockerBashOperator(
    task_id='repair_popular_pages_tables',
    dag=temp_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/popular-pages.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_popular_pages_tables.set_upstream(popular_pages)

repair_social_receiving_tables = DockerBashOperator(
    task_id='repair_social_receiving_tables',
    dag=temp_dag,
    docker_name="op-hbs2",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/social-receiving.sh -d {{ ds }} -m window -mt last-28 -p repair'
)

repair_social_receiving_tables.set_upstream(social_receiving)
repair_hive_tables.set_upstream([repair_dagg_tables, repair_dest_tables, repair_incoming_tables, repair_keywords_tables,
                                 repair_outgoing_tables, repair_popular_pages_tables, repair_ranks_tables,
                                 repair_sending_pages_tables, repair_social_receiving_tables, repair_sr_tables])
