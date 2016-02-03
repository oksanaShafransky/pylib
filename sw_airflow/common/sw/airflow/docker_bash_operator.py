import logging
import random
import subprocess

from airflow.operators.bash_operator import BashOperator
from airflow.utils import apply_defaults


class DockerBashOperator(BashOperator, object):
    ui_color = '#FFFF66'
    template_fields = ('bash_command', 'kill_cmd')
    cmd_template = '''docker run \
-H=tcp://{{ params.docker_gate }}:2375 \
-v {{ params.execution_dir }}:/tmp/dockexec/%(random)s \
-v /etc/localtime:/etc/localtime:ro \
-v /tmp/logs:/tmp/logs \
-v /var/lib/sss:/var/lib/sss \
-v /etc/localtime:/etc/localtime:ro \
-v /usr/bin:/opt/old_bin \
-v /var/run/similargroup:/var/run/similargroup \
--rm \
--sig-proxy=false \
--user=`id -u` \
-e DOCKER_GATE={{ docker_manager }} \
-e GELF_HOST="runsrv2.sg.internal" \
-e HOME=/tmp \
bigdata/centos6.cdh5.%(docker)s bash -c " \
 sudo mkdir -p {{ params.execution_dir }} && \
 sudo cp -r /tmp/dockexec/%(random)s/* {{ params.execution_dir }} && \
 %(bash_command)s"
'''

    kill_cmd_template = '''docker -H=tcp://{{ params.docker_gate }}:2375 rm -f %(container_name)s'''

    @apply_defaults
    def __init__(self, docker_name, bash_command, *args, **kwargs):
        super(DockerBashOperator, self).__init__(bash_command=None, *args, **kwargs)

        self.docker_name = docker_name

        rand = str(random.randint(10000, 99999))
        self.container_name = '''%(dag_id)s.%(task_id)s.%(rand)s''' % {'dag_id': self.dag.dag_id,
                                                                       'task_id': self.task_id, 'rand': rand}
        docker_command = DockerBashOperator.cmd_template % {'random': rand, 'container_name': self.container_name,
                                                            'docker': self.docker_name,
                                                            'bash_command': bash_command}
        self.kill_cmd = DockerBashOperator.kill_cmd_template % {'container_name': self.container_name}
        self.bash_command = docker_command

    def on_kill(self):
        logging.info('Killing container %s' % self.container_name)
        subprocess.call(['bash', '-c', self.kill_cmd])
        super(DockerBashOperator, self).on_kill()

    def set_upstream(self, task_or_task_list):
        super(DockerBashOperator, self).set_upstream(task_or_task_list)
        return self


class DockerBashOperatorFactory(object):
    def __init__(self,
                 base_data_dir=None,
                 core_command=None,
                 dag=None,
                 date_template=None,
                 docker_name=None,
                 mode=None,
                 mode_type=None,
                 script_path=None,
                 use_defaults=False,
                 additional_cmd_components=[]
                 ):
        self.base_data_dir = base_data_dir
        self.core_command = core_command
        self.dag = dag
        self.date_template = date_template
        self.docker_name = docker_name
        self.mode = mode
        self.mode_type = mode_type
        self.script_path = script_path
        self.additional_cmd_components = additional_cmd_components
        if not dag:
            raise DockerBashCommandBuilderException('target dag to register operator is mandatory')
        if use_defaults:
            self.fill_in_defaults()

    def fill_in_defaults(self):
        if not self.base_data_dir:
            self.base_data_dir = '''{{ params.base_data_dir }}'''
        if not self.mode:
            self.mode = '''{{ params.mode }}'''
        if not self.mode_type:
            self.mode_type = '''{{ params.mode_type }}'''
        if not self.date_template:
            self.date_template = '''{{ ds }}'''
        if not self.date_template:
            self.docker_name = '''{{ params.container_name }}'''
        return self

    def add_cmd_component(self, cmd_component):
        self.additional_cmd_components.append(cmd_component)
        return self

    def build(self, task_id, core_command=None, docker_name=None, dag_element_type='operator'):
        if core_command:
            full_command = core_command
        elif self.core_command:
            full_command = self.core_command
        else:
            raise DockerBashCommandBuilderException("Core bash command not set")

        if docker_name:
            docker_to_use = docker_name
        elif self.docker_name:
            docker_to_use = self.docker_name
        else:
            raise DockerBashCommandBuilderException('docker_name is mandatory, set in constructor or pass into build')

        if self.script_path:
            full_command = self.script_path + '/' + full_command

        if self.base_data_dir:
            full_command += ' -bd ' + self.base_data_dir
        else:
            raise DockerBashCommandBuilderException('base_data_dir is mandatory')

        if self.date_template:
            full_command += ' -d ' + self.date_template
        if self.mode:
            full_command += ' -m ' + self.mode
        if self.mode_type:
            full_command += ' -mt ' + self.mode_type

        full_command += ' ' + ' '.join(self.additional_cmd_components)

        # logging.info('Building %s.%s="%s"' % (self.dag.dag_id, task_id, full_command))
        if dag_element_type == 'operator':
            return DockerBashOperator(task_id=task_id, dag=self.dag, docker_name=docker_to_use,
                                      bash_command=full_command)
        elif dag_element_type == 'sensor':
            raise DockerBashCommandBuilderException('not supported yet')
        else:
            raise DockerBashCommandBuilderException('not supported')


class DockerBashCommandBuilderException(Exception):
    pass
