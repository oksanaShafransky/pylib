"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

__author__='cregev'

dag_args = {
    'owner': "devops-similarweb",
    'start_date': datetime(2015, 10, 27),
    'depends_on_past': False,
    'email': ['devops@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('devops-airflow_aliveness_check', default_args=dag_args , schedule_interval=timedelta(seconds=30))


templated_command = """
    /usr/bin/zabbix_sender -c /etc/zabbix/zabbix_agentd.conf -s airflow-a01.sg.internal --key airflow.is_alive_check --value $(date +%s)
"""

task = BashOperator(
    task_id='airflow_aliveness_check',
    bash_command=templated_command,
    dag=dag)
