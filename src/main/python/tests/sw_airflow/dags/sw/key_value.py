__author__ = 'Felix'

from datetime import datetime, timedelta
from airflow.models import DAG

from sw_airflow.common.airflow.key_value import *


dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(16, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_template_params = {'run_environment': 'STAGING'}

dag = DAG(dag_id='KeyValueTest', default_args=dag_args, params=dag_template_params)

# check success register
SUCCESS_TEST_KEY_PATH = 'test/key-val/suc'
register_success = KeyValueSetOperator(task_id='RegisterSuccess',
                                       dag=dag,
                                       path=SUCCESS_TEST_KEY_PATH,
                                       env='''{{ params.run_environment }}'''
                                       )

check_success = KeyValueSensor(task_id='ValidateSuccess',
                               dag=dag,
                               path=SUCCESS_TEST_KEY_PATH,
                               env='''{{ params.run_environment }}''',
                               execution_timeout=timedelta(minutes=1)
                               )
check_success.set_upstream(register_success)


# check freestyle value write
DATE_TEST_KEY_PATH = 'test/key-val/date'
write_date = KeyValueSetOperator(task_id='WriteDate',
                                 dag=dag,
                                 path=DATE_TEST_KEY_PATH,
                                 value='''{{ ds }}''',
                                 env='''{{ params.run_environment }}'''
                                 )

# composite
BASE_VALUES_PATH = 'test/key-val/base'
should_run = KeyValueCompoundDateSensor(task_id='CheckDate',
                                        dag=dag,
                                        env='''{{ params.run_environment }}''',
                                        key_list_path=BASE_VALUES_PATH,
                                        desired_date='''{{ ds }}''',
                                        key_root='/'.join(DATE_TEST_KEY_PATH.split('/')[:-1]),
                                        execution_timeout=timedelta(minutes=1)
                                        )
should_run.set_upstream(write_date)
