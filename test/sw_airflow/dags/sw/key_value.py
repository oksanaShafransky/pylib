__author__ = 'Felix'

from datetime import datetime
from airflow.models import DAG

from sw.airflow.key_value import *


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
                               env='''{{ params.run_environment }}'''
                               )
check_success.set_upstream(register_success)


# check freestyle value write

# composite
