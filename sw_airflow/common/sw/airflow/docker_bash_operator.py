import logging
import random
import subprocess

from airflow.operators.bash_operator import BashOperator
from airflow.utils import apply_defaults


class DockerBashOperator(BashOperator):
    ui_color = '#FFFF66'
    template_fields = ('bash_command', 'kill_cmd')
    cmd_template = '''docker -H=tcp://{{ params.docker_gate }}:2375 run\
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
runsrv/%(docker)s bash -c "sudo mkdir -p {{ params.execution_dir }} && sudo cp -r /tmp/dockexec/%(random)s/* {{ params.execution_dir }} && %(bash_command)s"
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


class DockerBashOperatorBuilder():
    def __init__(self):
        self.script_path = None
        self.core_command = None
        self.base_data_dir = None
        self.docker_name = None
        self.cmd_components = []
        self.dag = None
        self.date_template = None

    def clone(self):
        other=DockerBashOperatorBuilder()
        other.script_path = self.script_path
        other.core_command = self.core_command
        other.base_data_dir = self.base_data_dir
        other.docker_name = self.base_data_dir
        other.cmd_components = self.cmd_components
        other.dag = self.dag
        other.date_template = self.date_template
        return other

    def set_docker_name(self, docker_name):
        self.docker_name = docker_name
        return self

    def set_script_path(self, script_path):
        self.script_path = script_path
        return self

    def set_core_command(self, core_command):
        self.core_command = core_command
        return self

    def set_base_data_dir(self, base_data_dir):
        self.base_data_dir = base_data_dir
        return self

    def set_dag(self, dag):
        self.dag = dag
        return self

    def set_date_template(self, date_template):
        self.date_template = date_template
        return self

    def add_cmd_component(self, cmd_component):
        self.cmd_components.append(cmd_component)
        return self

    def reset_cmd_components(self):
        self.cmd_components = []
        return self

    def build(self, task_id, core_command=None):
        if core_command:
            full_command = core_command
        elif self.core_command:
            full_command = self.core_command
        else:
            raise DockerBashOperatorBuilderException("Core bash command not set")

        if self.script_path:
            full_command = self.script_path + '/' + full_command

        if self.date_template:
            full_command += ' -d ' + self.date_template

        full_command += ' -bd ' + self.base_data_dir
        full_command += ' ' + ' '.join(self.cmd_components)

        logging.info('Building %s.%s="%s"' % (self.dag.dag_id, task_id, full_command))
        return DockerBashOperator(task_id=task_id, dag=self.dag, docker_name=self.docker_name,
                                  bash_command=full_command)


class DockerBashOperatorBuilderException(Exception):
    pass
