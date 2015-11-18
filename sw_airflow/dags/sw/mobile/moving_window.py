__author__ = 'Iddo Aviram'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.sensors import ExternalTaskSensor

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator


DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'
WINDOW_MODE = 'window'
SNAPHOT_MODE = 'snapshot'
WINDOW_MODE_TYPE = 'last-28'
SNAPSHOT_MODE_TYPE = 'monthly'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(15, 11, 10),
    'depends_on_past': False,
    'email': ['iddo.aviram@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}


def generate_dags(mode):

    def is_window_dag():
        return mode == WINDOW_MODE

    def is_snapshot_dag():
        return mode == SNAPHOT_MODE

    dag_template_params_for_mode = dag_template_params.copy()
    mode_dag_template_params = {}

    if is_window_dag():
        mode_dag_template_params = {'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE}

    if is_snapshot_dag():
        mode_dag_template_params = {'mode': SNAPHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE}

    dag_template_params_for_mode.update(mode_dag_template_params)

    dag = DAG(dag_id='MobileAppsMovingWindow_' + mode, default_args=dag_args, params=dag_template_params_for_mode, schedule_interval=timedelta(days=1))

    mobile_daily_estimation = ExternalTaskSensor(external_dag_id='MobileDailyEstimation',
                                                  dag=dag,
                                                  task_id="MobileDailyEstimation",
                                                  external_task_id='FinishProcess')

    ########################
    # Prepare HBase Tables #
    ########################

    prepare_hbase_tables = \
        DockerBashOperator(task_id='PrepareHBaseTables',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/start-process.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p tables'''
                           )

    prepare_hbase_tables.set_upstream(mobile_daily_estimation)

    #####################
    # App Usage Pattern #
    #####################

    usage_pattern_calculation = \
        DockerBashOperator(task_id='AppsUsagePatternCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p calculation'''
                           )

    usage_pattern_calculation.set_upstream(mobile_daily_estimation)

    app_usage_pattern_store = \
        DockerBashOperator(task_id='AppsUsagePatternStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p app_store'''
                           )

    app_usage_pattern_store.set_upstream([usage_pattern_calculation,
                                          prepare_hbase_tables])


    usage_raw_totals = \
        DockerBashOperator(task_id='UsageRawTotals',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p raw_totals'''
                           )

    # dependency on usage_store is important since this job writes to the same cf and usage_calc checks if it is populated
    # so inversing this order may cause usage_calc to decide not to run
    usage_raw_totals.set_upstream(app_usage_pattern_store)

    category_usage = \
        DockerBashOperator(task_id='CategoryUsage',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p category_store'''
                           )

    category_usage.set_upstream([prepare_hbase_tables,
                                 usage_pattern_calculation])

    usage_pattern_category_leaders = \
        DockerBashOperator(task_id='UsagePatternCategoryLeaders',
                       dag=dag,
                       docker_name='''{{ params.cluster }}''',
                       bash_command='''{{ params.execution_dir }}/mobile/scripts/usagepatterns/usagepattern.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p category_leaders'''
                    )

    usage_pattern_category_leaders.set_upstream([prepare_hbase_tables,
                                                 app_usage_pattern_store,
                                                 usage_raw_totals])

    #################
    # App Retention #
    #################

    app_retention_calculation = \
        DockerBashOperator(task_id='AppRetentionCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p calc_apps'''
                           )

    app_retention_calculation.set_upstream(mobile_daily_estimation)

    app_churn_calculation = \
        DockerBashOperator(task_id='AppChurnCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p churn_calc'''
                           )

    app_churn_calculation.set_upstream(app_retention_calculation)

    smooth_app_retention = \
        DockerBashOperator(task_id='SmoothAppRetention',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p smooth_retention'''
                           )

    smooth_app_retention.set_upstream(app_churn_calculation)

    app_retention_precalculation = DummyOperator(task_id='AppRetentionPrecalculation',
                                      dag=dag
                                      )

    if is_window_dag():
        check_app_and_country_retention_estimation = \
            DockerBashOperator(task_id='CheckAppAndCountryRetentionEstimation',
                               dag=dag,
                               docker_name='''{{ params.cluster }}''',
                               bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/qa/app-retention/qa/checkAppAndCountryRetentionEstimation.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p check'''
                               )
        check_app_and_country_retention_estimation.set_upstream(smooth_app_retention)
        app_retention_precalculation.set_upstream(check_app_and_country_retention_estimation)
    else:
        app_retention_precalculation.set_upstream(smooth_app_retention)

    app_retention_categories_calculation = \
        DockerBashOperator(task_id='AppRetentionCategoriesCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p calc_cats'''
                           )

    app_retention_categories_calculation.set_upstream(app_retention_precalculation)

    app_retention_aggregate_categories = \
        DockerBashOperator(task_id='AppRetentionAggregateCategories',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p agg_cats'''
                           )

    app_retention_aggregate_categories.set_upstream(app_retention_categories_calculation)

    app_retention_categories = DummyOperator(task_id='AppRetentionCategories',
                                                 dag=dag
                                                 )
    app_retention_categories.set_upstream(app_retention_aggregate_categories)

    app_retention_store = \
        DockerBashOperator(task_id='AppRetentionStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p store_apps'''
                           )

    app_retention_store.set_upstream([prepare_hbase_tables,app_retention_precalculation])

    category_retention_store = \
        DockerBashOperator(task_id='CategoryRetentionStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p store_cats'''
                           )

    category_retention_store.set_upstream([prepare_hbase_tables,app_retention_categories])

    retention_store = DummyOperator(task_id='RetentionStore',
                                                 dag=dag
                                                 )
    retention_store.set_upstream([app_retention_store,category_retention_store])

    retention_leaders_calculation = \
        DockerBashOperator(task_id='RetentionLeadersCalculation',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p leaders_calc'''
                           )

    retention_leaders_calculation.set_upstream([prepare_hbase_tables,retention_store])

    retention_leaders_store = \
        DockerBashOperator(task_id='RetentionLeadersStore',
                           dag=dag,
                           docker_name='''{{ params.cluster }}''',
                           bash_command='''{{ params.execution_dir }}/mobile/scripts/app-retention/retention.sh -d {{ ds }} -bd {{ base_hdfs_dir }} -m {{ mode }} -mt {{ mode_type }} -p leaders_store'''
                           )

    retention_leaders_store.set_upstream(retention_leaders_calculation)

    retention_leaders = DummyOperator(task_id='RetentionLeaders',
                                   dag=dag
                                   )
    retention_leaders.set_upstream(retention_leaders_store)


    return dag

globals()['dag_apps_moving_window_snapshot'] = generate_dags(SNAPHOT_MODE)
globals()['dag_apps_moving_window_daily'] = generate_dags(WINDOW_MODE)