from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime

from sw.airflow.docker_bash_operator import DockerBashOperatorFactory
from sw.airflow.external_sensors import AdaptedExternalTaskSensor, AggRangeExternalTaskSensor

WINDOW_MODE = 'window'
SNAPSHOT_MODE = 'snapshot'
dag_args = {
    'owner': 'MobileWeb',
    'depends_on_past': False,
    'email': ['amitr@similarweb.com', 'barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2016, 2, 14),
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': '/similargroup/production',
                       'docker_gate': 'docker-a02.sg.internal',
                       'base_data_dir': '/similargroup/data/mobile-analytics',
                       'run_environment': 'PRODUCTION',
                       'cluster': 'mrp'
                       }

window_template_params = dag_template_params.copy()
window_template_params.update({'mode': WINDOW_MODE, 'mode_type': 'last-28'})
snapshot_template_params = dag_template_params.copy()
snapshot_template_params.update({'mode': SNAPSHOT_MODE, 'mode_type': 'monthly'})

snapshot_dag = DAG(dag_id='MobileWeb_Snapshot', default_args=dag_args, params=snapshot_template_params,
                   schedule_interval='@monthly')

window_dag = DAG(dag_id='MobileWeb_Window', default_args=dag_args, params=window_template_params,
                 schedule_interval='@daily')


def assemble_process(mode, dag):
    factory = DockerBashOperatorFactory(use_defaults=True,
                                        dag=dag,
                                        script_path='''{{ params.execution_dir }}/mobile/scripts/web''',
                                        additional_cmd_components=['-env main'])

    daily_redist = AdaptedExternalTaskSensor(external_dag_id='MobileWeb_Daily', dag=dag,
                                             task_id="MobileWeb_Daily_redist",
                                             external_task_id='redist')

    prepare_hbase_tables = factory.build(task_id='prepare_hbase_tables',
                                         core_command='../start-process.sh -p tables -fl MOBILE_WEB')

    adjust_store = add_adjust_store(dag, factory, [prepare_hbase_tables, daily_redist])
    popular_pages_top_store = add_popular_pages(dag, factory, prepare_hbase_tables)
    calc_subdomains = add_calc_subdomains(factory, [prepare_hbase_tables, daily_redist])

    mobile_web = DummyOperator(task_id=dag.dag_id, dag=dag, sla=timedelta(hours=12))
    mobile_web.set_upstream([adjust_store, calc_subdomains, popular_pages_top_store])

    if mode == SNAPSHOT_MODE:
        predict_validate_preparation = \
            factory.build(task_id='predict_validate_preparation',
                          core_command='second_stage_tests.sh -wenv daily-cut -p prepare_total_device_count')

        predict_validate = \
            factory.build(task_id='predict_validate',
                          core_command='second_stage_tests.sh -wenv daily-cut '
                                       '-p prepare_predictions_for_test,verify_predictions')
        predict_validate.set_upstream([predict_validate_preparation, daily_redist])

        first_stage_agg = AdaptedExternalTaskSensor(external_dag_id='MobileWeb_Daily', dag=dag,
                                                    task_id="MobileWeb_Daily_first_stage_agg",
                                                    external_task_id='first_stage_agg')
        compare_est_to_qc = factory.build(task_id='compare_est_to_qc',
                                          core_command='compare_estimations_to_qc.sh -sm')
        compare_est_to_qc.set_upstream(first_stage_agg)

        estimation = AdaptedExternalTaskSensor(external_dag_id='MobileWeb_Estimation', dag=dag,
                                               task_id="MobileWeb_Estimation",
                                               external_task_id='Estimation')

        first_stage_agg_for_model = factory.build(task_id='first_stage_agg_for_model',
                                                  core_command='first_stage_agg.sh')
        first_stage_agg_for_model.set_upstream(estimation)

        mobile_web.set_upstream([predict_validate, compare_est_to_qc, first_stage_agg_for_model])


def add_calc_subdomains(factory, upstreams):
    calc_subdomains = factory.build(task_id='calc_subdomains', core_command='calc_subdomains.sh', timeout=60 * 60 * 24)
    calc_subdomains.set_upstream(upstreams)
    return calc_subdomains


def add_adjust_store(dag, factory, upstreams):
    sum_ww = AggRangeExternalTaskSensor(external_dag_id='MobileWeb_Daily', dag=dag,
                                        task_id="MobileWeb_Daily_sum_ww",
                                        external_task_id='sum_ww',
                                        timeout=60 * 60 * 24,
                                        agg_mode=dag.params.get('mode_type')
                                        )
    adjust_store = factory.build(task_id='adjust_store', core_command='adjust_est.sh -p store -ww')
    adjust_store.set_upstream([sum_ww] + upstreams)
    return adjust_store


def add_popular_pages(dag, factory, prepare_hbase_tables):
    """
    adds popular pages calculation to the dag. splitting this to separate function to put it aside
    for the cleanup of the rest. afterward worth considering inlinening this function back
    :param prepare_hbase_tables: dependency that creates the relevant hbase table
    :param factory: DockerBashOperatorFactory
    :param dag: dag to enrich
    :returns last operator in the flow to tie into last DummyOperator of the dag
    """
    mobile_daily_aggregation = AdaptedExternalTaskSensor(external_dag_id='Mobile_Preliminary',
                                                         dag=dag,
                                                         task_id='Mobile_DailyAggregation',
                                                         external_task_id='DailyAggregation',
                                                         timeout=60 * 60 * 24)
    popular_pages_agg = factory.build(task_id='popular_pages_agg',
                                      core_command='popular_pages.sh -p aggregate_popular_pages')
    popular_pages_agg.set_upstream(mobile_daily_aggregation)
    popular_pages_top_store = factory.build(task_id='popular_pages_top_store',
                                            core_command='popular_pages.sh -p top_popular_pages')
    popular_pages_top_store.set_upstream([prepare_hbase_tables, popular_pages_agg])
    return popular_pages_top_store


assemble_process(SNAPSHOT_MODE, snapshot_dag)
assemble_process(WINDOW_MODE, window_dag)
