import copy

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta, datetime

from sw.airflow.docker_bash_operator import DockerBashOperatorFactory

WINDOW_MODE = 'window'
WINDOW_MODE_TYPE = 'last-28'
SNAPSHOT_MODE = 'snapshot'
SNAPSHOT_MODE_TYPE = 'monthly'

dag_args = {
    'owner': 'MobileWeb',
    'depends_on_past': True,
    'email': ['barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2016, 2, 8),
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
window_template_params.update({'mode': WINDOW_MODE, 'mode_type': WINDOW_MODE_TYPE})
snapshot_template_params = dag_template_params.copy()
snapshot_template_params.update({'mode': SNAPSHOT_MODE, 'mode_type': SNAPSHOT_MODE_TYPE})

snapshot_dag = DAG(dag_id='MobileWeb_Snapshot', default_args=dag_args, params=snapshot_template_params,
                   schedule_interval='@monthly')

window_dag = DAG(dag_id='MobileWeb_Window', default_args=dag_args, params=window_template_params,
                 schedule_interval='@daily')


def assemble_process(mode, dag, sum_ww_value_size):
    estimation = ExternalTaskSensor(external_dag_id='MobileWeb_Estimation', dag=dag,
                                    task_id="MobileWeb_Estimation",
                                    external_task_id='Estimation')
    desktop_estimation_aggregation = ExternalTaskSensor(external_dag_id='Desktop_DailyEstimation',
                                                        dag=dag,
                                                        task_id='SumEstimation',
                                                        external_task_id='MonthlySumEstimationParameters')

    should_run_mw = DummyOperator(dag=dag, task_id='should_run_mw')
    should_run_mw.set_upstream([estimation, desktop_estimation_aggregation])

    factory = DockerBashOperatorFactory(use_defaults=True,
                                        dag=dag,
                                        script_path='''{{ params.execution_dir }}/mobile/scripts/web''',
                                        additional_cmd_components=['-env main'])

    prepare_hbase_tables = factory.build(task_id='prepare_hbase_tables',
                                         core_command='../start-process.sh -p tables -fl MOBILE_WEB')
    prepare_hbase_tables.set_upstream(should_run_mw)

    popular_pages_agg = factory.build(task_id='popular_pages_agg',
                                      core_command='popular_pages.sh -p aggregate_popular_pages')
    popular_pages_agg.set_upstream(should_run_mw)

    popular_pages_top_store = factory.build(task_id='popular_pages_top_store',
                                            core_command='popular_pages.sh -p top_popular_pages')
    popular_pages_top_store.set_upstream([prepare_hbase_tables, popular_pages_agg])

    gaps_filler = factory.build(task_id='gaps_filler',
                                core_command='airflow_mobile_web_gaps_filler.sh')
    gaps_filler.set_upstream(should_run_mw)

    # ############################################################################################################
    # the reason for new factory here is that we control whether to use new algo through mode and mode type params

    new_algo_factory = copy.copy(factory)
    new_algo_factory.mode = 'window'
    new_algo_factory.mode_type = 'last-28'

    first_stage_agg = new_algo_factory.build(task_id='first_stage_agg',
                                             core_command='first_stage_agg.sh')
    first_stage_agg.set_upstream(should_run_mw)

    adjust_calc_intermediate = \
        new_algo_factory.build(task_id='adjust_calc_intermediate',
                               core_command='adjust_est.sh -p prepare_data,predict -wenv daily-cut')
    adjust_calc_intermediate.set_upstream(first_stage_agg)

    adjust_calc_redist = new_algo_factory.build(task_id='adjust_calc_redist',
                                                core_command='adjust_est.sh -p redist -wenv daily-cut')
    adjust_calc_redist.set_upstream([gaps_filler, adjust_calc_intermediate])
    # ###########################################################################################################

    check_daily_estimations = factory.build(task_id='check_daily_estimations',
                                            core_command='check_daily_estimations.sh')
    check_daily_estimations.set_upstream(adjust_calc_redist)

    calc_subdomains = factory.build(task_id='calc_subdomains',
                                    core_command='calc_subdomains.sh')
    calc_subdomains.set_upstream([adjust_calc_redist, prepare_hbase_tables])

    # ww value aggregation
    sum_ww_factory = copy.copy(factory)
    sum_ww_factory.mode = 'daily'

    sum_ww_all = []
    for day_to_calculate in range(0, sum_ww_value_size):
        sum_ww_i = sum_ww_factory.build(task_id='sum_ww_day_%s' % day_to_calculate,
                                        date_template='''{{ macros.ds_add(ds,-%d) }}''' % day_to_calculate,
                                        core_command='sum_ww.sh')
        sum_ww_i.set_upstream(adjust_calc_redist)
        sum_ww_all.append(sum_ww_i)

    # load to Hbase
    adjust_store = factory.build(task_id='adjust_store',
                                 core_command='adjust_est.sh -p store -ww')
    adjust_store.set_upstream([prepare_hbase_tables] + sum_ww_all)

    # FINISH WINDOW PART
    mobile_web = DummyOperator(task_id=dag.dag_id, dag=dag)
    mobile_web.set_upstream([adjust_store, calc_subdomains, popular_pages_top_store])

    if mode == SNAPSHOT_MODE:
        predict_validate_preparation = \
            factory.build(task_id='predict_validate_preparation',
                          core_command='second_stage_tests.sh -wenv daily-cut -p prepare_total_device_count')
        predict_validate_preparation.set_upstream(should_run_mw)

        predict_validate = \
            factory.build(task_id='predict_validate',
                          core_command='second_stage_tests.sh -wenv daily-cut '
                                       '-p prepare_predictions_for_test,verify_predictions'
                          )
        predict_validate.set_upstream([predict_validate_preparation, adjust_calc_redist])

        compare_est_to_qc = factory.build(task_id='compare_est_to_qc',
                                          core_command='compare_estimations_to_qc.sh -sm')
        compare_est_to_qc.set_upstream(first_stage_agg)

        first_stage_agg_for_model = factory.build(task_id='first_stage_agg_for_model',
                                                  core_command='first_stage_agg.sh')
        first_stage_agg_for_model.set_upstream(should_run_mw)

        mobile_web.set_upstream([predict_validate, compare_est_to_qc, first_stage_agg_for_model])


assemble_process(SNAPSHOT_MODE, snapshot_dag, sum_ww_value_size=31)
assemble_process(WINDOW_MODE, window_dag, sum_ww_value_size=int(WINDOW_MODE_TYPE.split('-')[1]))
