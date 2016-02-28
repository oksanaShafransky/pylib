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
                       'docker_gate': 'docker-a02.sg.internal',
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

prepare_learningset = factory.build(task_id='prepare_learningset', core_command='adclassification.sh -s prepare-learningset')
prepare_learningset.set_upstream([desktop_window])
generate_newsites = factory.build(task_id='generate_newsites', core_command='adclassification.sh -s generate-newsites')
generate_newsites.set_upstream([desktop_window])

stage1_tasks = [prepare_learningset, generate_newsites]

stage1_complete = DummyOperator(task_id='stage1_complete', dag=dag)
stage1_complete.set_upstream(stage1_tasks)

generate_popular_pages = factory.build(task_id='generate_popular_pages', core_command='adclassification.sh -s generate-popular-pages')
generate_popular_pages.set_upstream(stage1_complete)
generate_outgoing = factory.build(task_id='generate_outgoing', core_command='adclassification.sh -s generate-outgoing')
generate_outgoing.set_upstream(stage1_complete)
generate_incoming = factory.build(task_id='generate_incoming', core_command='adclassification.sh -s generate-incoming')
generate_incoming.set_upstream(stage1_complete)
generate_distro = factory.build(task_id='generate_distro', core_command='adclassification.sh -s generate-distro')
generate_distro.set_upstream(stage1_complete)
generate_divmain = factory.build(task_id='generate_divmain', core_command='adclassification.sh -s generate-divmain')
generate_divmain.set_upstream(stage1_complete)
generate_values = factory.build(task_id='generate_values', core_command='adclassification.sh -s generate-values')
generate_values.set_upstream(stage1_complete)
generate_had_sub = factory.build(task_id='generate_had_sub', core_command='adclassification.sh -s generate-had-sub')
generate_had_sub.set_upstream(stage1_complete)

stage2_tasks = [generate_popular_pages, generate_outgoing, generate_incoming, generate_distro,
                generate_divmain, generate_values, generate_had_sub]

stage2_complete = DummyOperator(task_id='stage2_complete', dag=dag)
stage2_complete.set_upstream(stage2_tasks)

generate_keywords = factory.build(task_id='generate_keywords', core_command='adclassification.sh -s generate-keywords')
generate_keywords.set_upstream(stage2_complete)
generate_intra_outgoing = factory.build(task_id='generate_intra_outgoing', core_command='adclassification.sh -s generate-intra-outgoing')
generate_intra_outgoing.set_upstream(stage2_complete)
generate_intra_incoming = factory.build(task_id='generate_intra_incoming', core_command='adclassification.sh -s generate-intra-incoming')
generate_intra_incoming.set_upstream(stage2_complete)
generate_paid_outgoing = factory.build(task_id='generate_paid_outgoing', core_command='adclassification.sh -s generate-paid-outgoing')
generate_paid_outgoing.set_upstream(stage2_complete)
generate_paid_incoming = factory.build(task_id='generate_paid_incoming', core_command='adclassification.sh -s generate-paid-incoming')
generate_paid_incoming.set_upstream(stage2_complete)
generate_ppage_words = factory.build(task_id='generate_ppage_words', core_command='adclassification.sh -s generate-ppage-words')
generate_ppage_words.set_upstream(stage2_complete)
generate_ppage_length = factory.build(task_id='generate_ppage_length', core_command='adclassification.sh -s generate-ppage-length')
generate_ppage_length.set_upstream(stage2_complete)

stage3_tasks = [generate_keywords, generate_intra_outgoing, generate_intra_incoming, generate_paid_outgoing,
                generate_paid_incoming, generate_ppage_words, generate_ppage_length]

generate_all = factory.build(task_id='generate_all', core_command='adclassification.sh -s generate-all')
generate_all.set_upstream(stage3_tasks)

classification = factory.build(task_id='classification', core_command='adclassification.sh -s classification')
classification.set_upstream([generate_all])

ad_classification_complete = DummyOperator(task_id='ad_classification_complete', dag=dag, sla=timedelta(days=2))
ad_classification_complete.set_upstream([classification])
