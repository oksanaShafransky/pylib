__author__ = 'Felix'

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.slack_operator import SlackAPIPostOperator

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(16, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_template_params = {'notify_channel': '#airflow_test_alerts'}

slack_user = 'felixv@similargroup.com'
slack_token = 'xoxp-3292636427-3379961773-18271129910-d7ee44f43e'
image_url = 'http://scontent.cdninstagram.com/hphotos-xfa1/t51.2885-19/s150x150/11849321_133631966980595_1160200665_a.jpg'

dag = DAG(dag_id='SlackDag', default_args=dag_args, params=dag_template_params)

simple_post = SlackAPIPostOperator(task_id='PostToChannel',
                                   dag=dag,
                                   token=slack_token,
                                   channel=dag_template_params['notify_channel'],
                                   username=slack_user,
                                   icon_url=image_url,
                                   text='''Felix, you are so awesome on {{ ds }}'''
                                   )


