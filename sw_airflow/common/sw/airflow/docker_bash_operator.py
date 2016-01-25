import logging
import random
import subprocess

from airflow.operators.bash_operator import BashOperator
from airflow.utils import apply_defaults


class DockerBashOperator(BashOperator):
    ui_color = '#FFFF66'
    template_fields = ('bash_command', 'docker_name', 'kill_cmd')
    cmd_template = '''docker -H=tcp://{{ params.docker_gate }}:2375 run       \
-v {{ params.execution_dir }}:/tmp/dockexec/%(random)s        \
-v /etc/localtime:/etc/localtime:ro                           \
-v /tmp/logs:/tmp/logs                                        \
-v /var/lib/sss:/var/lib/sss                                  \
-v /etc/localtime:/etc/localtime:ro                           \
-v /usr/bin:/opt/old_bin                                      \
-v /var/run/similargroup:/var/run/similargroup                \
--rm                                                          \
--sig-proxy=false                                             \
--user=`id -u`                                                \
-e DOCKER_GATE={{ docker_manager }}                           \
-e GELF_HOST="runsrv2.sg.internal"                            \
-e HOME=/tmp                                                  \
runsrv/%(docker)s bash -c "sudo mkdir -p {{ params.execution_dir }} && sudo cp -r /tmp/dockexec/%(random)s/* {{ params.execution_dir }} && %(bash_command)s"
    '''

    kill_cmd_template = '''docker -H=tcp://{{ params.docker_gate }}:2375 rm -f %(container_name)s'''
    kill_cmd = ''

    @apply_defaults
    def __init__(self, docker_name, bash_command, *args, **kwargs):
        super(DockerBashOperator, self).__init__(bash_command=None, *args, **kwargs)

        rand = str(random.randint(10000, 99999))

        self.docker_name = docker_name
        self.container_name = '''%(dag_id)s.%(task_id)s.%(rand)s''' % {'dag_id': self.dag.dag_id, 'task_id': self.task_id, 'rand': rand}

        docker_command = DockerBashOperator.cmd_template % {'random': rand, 'container_name': self.container_name, 'docker': self.docker_name,
                                                                     'bash_command': bash_command}

        self.kill_cmd = DockerBashOperator.kill_cmd_template % {'container_name': self.container_name}
        self.bash_command = docker_command

    def on_kill(self):
        logging.info('Killing container %s' % self.container_name)

        subprocess.call(['bash', '-c', self.kill_cmd])

        super(DockerBashOperator, self).on_kill()