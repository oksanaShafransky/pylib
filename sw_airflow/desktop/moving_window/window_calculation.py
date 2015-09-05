__author__ = 'jeniag'

from daily_calculation import *
from sw_airflow.desktop.moving_window.dag import temp_dag

# Create tables in HBase
hbase_tables = DockerBashOperator(
    task_id='hbase_tables',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -m window -mt last-28 -p tables'
)
hbase_tables.set_upstream(should_run_window)


# Daily incoming data
daily_incoming = DockerBashOperator(
    task_id='daily_incoming',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/daily/dailyIncoming.sh -d {{ ds }} -m window -mt last-28 '
)

daily_incoming.set_upstream(dest_all)
daily_incoming.set_upstream(hbase_tables)

monthly_sum_estimation_parameters = DockerBashOperator(
    task_id='monthly_sum_estimation_parameters',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -m window -mt last-28 -p monthly_sum_estimation_parameters'
)

monthly_sum_estimation_parameters.set_upstream(dest_all)

sum_special_referrer_values = DockerBashOperator(
    task_id='sum_special_referrer_values',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -m window -mt last-28 -p sum_special_referrer_values'
)

sum_special_referrer_values.set_upstream(dagg_all)

site_country_special_referrer_distribution = DockerBashOperator(
    task_id='site_country_special_referrer_distribution',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -m window -mt last-28 -p site_country_special_referrer_distribution'
)

site_country_special_referrer_distribution.set_upstream(monthly_sum_estimation_parameters)
site_country_special_referrer_distribution.set_upstream(sum_special_referrer_values)

traffic_distro = DockerBashOperator(
    task_id='traffic_distro',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/start-month.sh -d {{ ds }} -m window -mt last-28 -p export_traffic_distro_from_hbase'
)

traffic_distro.set_upstream(site_country_special_referrer_distribution)

estimate_incoming = DockerBashOperator(
    task_id='estimate_incoming',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ ds }} -m window -mt last-28 -p estimate_incoming'
)

estimate_incoming.set_upstream(site_country_special_referrer_distribution)

incoming = DockerBashOperator(
    task_id='incoming',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/incoming.sh -d {{ ds }} -m window -mt last-28'
)

incoming.set_upstream(estimate_incoming)

outgoing = DockerBashOperator(
    task_id='outgoing',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/outgoing.sh -d {{ ds }} -m window -mt last-28'
)

outgoing.set_upstream(estimate_incoming)

keywords = DockerBashOperator(
    task_id='keywords',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/incoming-keywords.sh -d {{ ds }} -m window -mt last-28'
)

keywords.set_upstream(site_country_special_referrer_distribution)

social_receiving = DockerBashOperator(
    task_id='social_receiving',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/social-receiving.sh -d {{ ds }} -m window -mt last-28'
)

social_receiving.set_upstream(site_country_special_referrer_distribution)

sending_pages = DockerBashOperator(
    task_id='sending_pages',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/sending-pages.sh -d {{ ds }} -m window -mt last-28'
)

sending_pages.set_upstream(estimate_incoming)

misc = DockerBashOperator(
    task_id='misc',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/misc.sh -d {{ ds }} -m window -mt last-28 -p calculate_subdomains,insert_worldwide_traffic,insert_daily_data'
)

misc.set_upstream(monthly_sum_estimation_parameters)
misc.set_upstream(hbase_tables)

ranks = DockerBashOperator(
    task_id='ranks',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ ds }} -m window -mt last-28 -p create_info_table,calculate_ranks,export_top_lists,topsites_for_testing'
)

ranks.set_upstream(monthly_sum_estimation_parameters)
ranks.set_upstream(hbase_tables)

check_distros = DockerBashOperator(
    task_id='check_distros',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/daily/qa/checkSiteDistro.sh -d {{ ds }} -m window -mt last-28 {{ transients }} '
)

check_distros.set_upstream(traffic_distro)

check_customers_est = DockerBashOperator(
    task_id='check_customers_est',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/daily/qa/checkCustomerEstimationPerSite.sh -d {{ ds }} -m window -mt last-28, {{ transients }} '
)

check_customers_est.set_upstream(dest_all)

check_customer_distros = DockerBashOperator(
    task_id='check_customer_distros',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/daily/qa/checkCustomerSiteDistro.sh -d {{ ds }} -m window -mt last-28 {{ transients }} '
)

check_customer_distros.set_upstream(traffic_distro)

popular_pages = DockerBashOperator(
    task_id='popular_pages',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/popular-pages.sh -d {{ ds }} -m window -mt last-28 '
)

popular_pages.set_upstream(dagg_all)
popular_pages.set_upstream(hbase_tables)

export_rest = DockerBashOperator(
    task_id='export_rest',
    dag=temp_dag,
    docker_name="{{ params.default_docker }}",
    bash_command='{{ params.execution_dir }}/analytics/scripts/monthly/ranks.sh -d {{ ds }} -m window -mt last-28 -p export_rest'
)

export_rest.set_upstream(ranks)

calculation_done = DummyOperator(task_id='all_calculation', dag=temp_dag)
calculation_done.set_upstream([incoming, outgoing, ranks, misc, keywords, export_rest, popular_pages, daily_incoming])
