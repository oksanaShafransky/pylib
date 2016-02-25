from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime

from sw.airflow.docker_bash_operator import DockerBashOperatorFactory
from sw.airflow.external_sensors import AdaptedExternalTaskSensor

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
desktop_window = AdaptedExternalTaskSensor(external_dag_id='Desktop_MovingWindow_Window',
                                           dag=dag,
                                           task_id='desktop_window',
                                           external_task_id='MovingWindow_Window',
                                           timeout=60 * 60 * 24)

stage1_1 = factory.build(task_id='stage1_1', core_command='adclassification.sh -s stage1')
stage1_1.set_upstream(desktop_window)

stage1_2 = factory.build(task_id='stage1_2', core_command='adclassification.sh -s stage2')
stage1_2.set_upstream(desktop_window)

stage1_3 = factory.build(task_id='stage1_3', core_command='adclassification.sh -s stage1')
stage1_3.set_upstream(desktop_window)

stage1_4 = factory.build(task_id='stage1_4', core_command='adclassification.sh -s stage2')
stage1_4.set_upstream(desktop_window)
stage1_tasks = [stage1_1, stage1_2, stage1_3, stage1_4]

stage1_complete = DummyOperator(task_id='stage1_complete', dag=dag)
stage1_complete.set_upstream(stage1_tasks)

stage2_1 = factory.build(task_id='stage2_1', core_command='adclassification.sh -s stage2')
stage2_1.set_upstream(stage1_complete)
stage2_2 = factory.build(task_id='stage2_2', core_command='adclassification.sh -s stage2')
stage2_2.set_upstream(stage1_complete)

ad_classification_complete = DummyOperator(task_id='ad_classification_complete', dag=dag, sla=timedelta(days=2))
ad_classification_complete.set_upstream([stage2_1, stage2_2])
