__author__ = 'Amit Rom'

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from sw.airflow.airflow_etcd import *
from sw.airflow.operators import DockerBashOperator

from airflow.operators.bash_operator import BashOperator
from airflow.utils import apply_defaults
import time
import logging
import logging
import sys
import subprocess
from tempfile import gettempdir, NamedTemporaryFile
from tempfile import gettempdir, NamedTemporaryFile

from airflow.utils import AirflowException
from airflow.models import BaseOperator
from airflow.utils import apply_defaults, TemporaryDirectory


DEFAULT_EXECUTION_DIR = '/similargroup/production'
BASE_DIR = '/similargroup/data/mobile-analytics'
DOCKER_MANAGER = 'docker-a02.sg.internal'
DEFAULT_CLUSTER = 'mrp'

ETCD_ENV_ROOT = {'STAGE': 'v1/dev', 'PRODUCTION': 'v1/production'}

dag_args = {
    'owner': 'similarweb',
    'start_date': datetime(2015, 12, 1),
    'depends_on_past': False, # Amit fix
    'email': ['amitr@similarweb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=15)
}

# amit test
# assign name & use on_kill to remove docker? yep
class CleanableDockerBashOperator(BashOperator):
    ui_color = '#FFFF66'
    template_fields = ('bash_command', 'docker_name')
    cmd_template = '''docker -H=tcp://{{ params.docker_gate }}:2375 run       \
-v {{ params.execution_dir }}:/tmp/dockexec/%(random)s        \
-v /etc/localtime:/etc/localtime:ro                           \
-v /tmp/logs:/tmp/logs                                        \
-v /var/lib/sss:/var/lib/sss                                  \
-v /etc/localtime:/etc/localtime:ro                           \
-v /usr/bin:/opt/old_bin                                      \
-v /var/run/similargroup:/var/run/similargroup                \
--rm                                                          \
--name=%(container_name)s                                     \
--sig-proxy=false                                             \
--user=`id -u`                                                \
-e DOCKER_GATE={{ docker_manager }}                           \
-e GELF_HOST="runsrv2.sg.internal"                            \
-e HOME=/tmp                                                  \
runsrv/%(docker)s bash -c "sudo mkdir -p {{ params.execution_dir }} && sudo cp -r /tmp/dockexec/%(random)s/* {{ params.execution_dir }} && %(bash_command)s"
    '''

    @apply_defaults
    def __init__(self, docker_name, bash_command, *args, **kwargs):
        super(CleanableDockerBashOperator, self).__init__(bash_command=None, *args, **kwargs)

        self.docker_name = docker_name

        random = str(datetime.utcnow().strftime('%s'))

        logging.info('Dag id %s, task id %s, date %s' % (self.dag.dag_id, self.task_id, random))

        self.container_name = '''%(dag_id)s_%(task_id)s_%(date)s''' % {'dag_id': self.dag.dag_id, 'task_id': self.task_id, 'date': random}

        logging.info('Container name is %s' % self.container_name)

        docker_command = CleanableDockerBashOperator.cmd_template % {'random': random, 'container_name': self.container_name, 'docker': self.docker_name,
                                                            'bash_command': bash_command}


        self.bash_command=docker_command

    def on_kill(self):
        logging.info('Killing container %s' % self.container_name)

        subprocess.call(['bash', '-c', 'docker -H=tcp://{{ params.docker_gate }}:2375 rm -f %s' % self.container_name])

        super(CleanableDockerBashOperator, self).on_kill()

# amit test


dag_template_params = {'execution_dir': DEFAULT_EXECUTION_DIR, 'docker_gate': DOCKER_MANAGER,
                       'base_hdfs_dir': BASE_DIR, 'run_environment': 'PRODUCTION', 'cluster': DEFAULT_CLUSTER}

dag = DAG(dag_id='MobileWebReferralsDailyPreliminary', default_args=dag_args, params=dag_template_params, schedule_interval=timedelta(days=1))


opera_raw_data_ready = EtcdSensor(task_id='OperaRawDataReady',
                                    dag=dag,
                                    root=ETCD_ENV_ROOT[dag_template_params['run_environment']],
                                    path='''services/opera-mini-s3/daily/{{ ds }}'''
)


test = CleanableDockerBashOperator(task_id='TestKillOperator',
                                             dag=dag,
                                             docker_name='''{{ params.cluster }}''',
                                             bash_command='''sleep 5m'''
                                             )


filter_malformed_events = DockerBashOperator(task_id='FilterMalformedEvents',
                                     dag=dag,
                                     docker_name='''{{ params.cluster }}''',
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/preliminary.sh -d {{ ds }} -p filter_malformed_events -env main'''
)


filter_malformed_events.set_upstream(test)
filter_malformed_events.set_upstream(opera_raw_data_ready)


extract_invalid_users = DockerBashOperator(task_id='ExtractInvalidUsers',
                                    dag=dag,
                                    docker_name=DEFAULT_CLUSTER,
                                    bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/preliminary.sh -d {{ ds }} -p filter_users -env main'''
)
extract_invalid_users.set_upstream(filter_malformed_events)


filter_invalid_users = DockerBashOperator(task_id='FilterInvalidUsers',
                                     dag=dag,
                                     docker_name=DEFAULT_CLUSTER,
                                     bash_command='''{{ params.execution_dir }}/mobile/scripts/web/referrals/preliminary.sh -d {{ ds }} -p filter_invalid_users_from_events -env main'''
)
filter_invalid_users.set_upstream(extract_invalid_users)

mobile_web_referrals_preliminary = DummyOperator(task_id='MobileWebReferralsDailyPreliminary', dag=dag)
mobile_web_referrals_preliminary.set_upstream(filter_invalid_users)