from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime

from sw.airflow.docker_bash_operator import DockerBashOperatorFactory

dag_args = {
    'owner': 'SpecialReferrers',
    'depends_on_past': False,
    'email': ['philipk@similarweb.com', 'nataliea@similarweb.com', 'barakg@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2016, 2, 20),
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag_template_params = {'execution_dir': '/similargroup/production',
                       'docker_gate': 'docker-a01.sg.internal',
                       'base_data_dir': '/similargroup/data/analytics',
                       'cluster': 'mrp'
                       }

dag = DAG(dag_id='SR_AdClassification', schedule_interval='0 0 25 * *', default_args=dag_args,
          params=dag_template_params)

factory = DockerBashOperatorFactory(
        dag=dag,
        date_template='''{{ ds }}''',
        cluster='''{{ params.cluster }}''',
        base_data_dir='''{{ params.base_data_dir }}''',
        script_path='''{{ params.execution_dir }}/analytics/scripts/monthly/adclassification''',
)

ad_classification = factory.build(task_id='ad_classification', core_command='adclassification.sh')

ad_classification_complete = DummyOperator(task_id='ad_classification_complete', dag=dag)
ad_classification_complete.set_upstream([ad_classification])
