from airflow.operators.python_operator import PythonOperator
import logging
from subprocess import PIPE, STDOUT, Popen
from tempfile import NamedTemporaryFile, gettempdir

from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import TemporaryDirectory, apply_defaults, State
from datetime import datetime


class BashSensor(BaseSensorOperator):
    """
    Runs bash script and checks if it succeeds
    """

    ui_color = '#C2F0C2'
    template_fields = ('bash_command',)
    template_ext = ('.sh', '.bash',)

    @apply_defaults
    def __init__(self, bash_command, env=None, poke_interval=60 * 5, timeout=60 * 60 * 24 * 7, *args, **kwargs):
        super(BashSensor, self).__init__(poke_interval=poke_interval, timeout=timeout, *args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.sp = None

    def on_kill(self):
        if self.sp:
            logging.info('Sending SIGTERM signal to bash subprocess')
            self.sp.terminate()

    def run_bash(self):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        bash_command = self.bash_command
        logging.info("tmp dir root location: \n" + gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:
                f.write(bash_command)
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                logging.info("Temporary script "
                             "location :{0}".format(script_location))
                logging.info("Running command: " + bash_command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env)

                self.sp = sp

                logging.info("Output:")
                for line in iter(sp.stdout.readline, ''):
                    logging.info(line.strip())
                sp.wait()
                logging.info("Command exited with "
                             "return code {0}".format(sp.returncode))

                if sp.returncode:
                    return False
                return True

    def poke(self, context):
        logging.info('Running check')
        return self.run_bash()


class DockerBashOperator(BashOperator):
    ui_color = '#FFFF66'
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

    @apply_defaults
    def __init__(self, docker_name, bash_command, *args, **kwargs):
        random_string = str(datetime.utcnow().strftime('%s'))
        docker_command = DockerBashOperator.cmd_template % {'random': random_string, 'docker': docker_name,
                                                            'bash_command': bash_command}
        super(DockerBashOperator, self).__init__(bash_command=docker_command, *args, **kwargs)


class DockerBashSensor(BashSensor):
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

    @apply_defaults
    def __init__(self, docker_name, bash_command, *args, **kwargs):
        random_string = str(datetime.utcnow().strftime('%s'))
        docker_command = DockerBashOperator.cmd_template % {'random': random_string, 'docker': docker_name,
                                                            'bash_command': bash_command}
        super(DockerBashSensor, self).__init__(bash_command=docker_command, *args, **kwargs)


class CopyHbaseTableOperator(BashOperator):
    ui_color = '#0099FF'
    cmd_template = '''source {{ params.execution_dir }}/scripts/infra.sh
hbasecopy %(source_cluster)s %(target_cluster)s %(table_name)s
    '''

    @apply_defaults
    def __init__(self, source_cluster, target_cluster, table_name_template, *args, **kwargs):
        docker_command = CopyHbaseTableOperator.cmd_template % {'source_cluster': source_cluster,
                                                                'target_cluster': target_cluster,
                                                                'table_name': table_name_template}
        super(CopyHbaseTableOperator, self).__init__(bash_command=docker_command, *args, **kwargs)


class SuccedOrSkipOperator(PythonOperator):
    ui_color = '#CC6699'

    def execute(self, context):
        skip_list, success_list = super(SuccedOrSkipOperator, self).execute(context)

        logging.info("Skipping taks: " + str(skip_list))
        session = settings.Session()
        for task in skip_list:
            ti = TaskInstance(
                task, execution_date=context['ti'].execution_date)
            ti.state = State.SKIPPED
            ti.start_date = datetime.now()
            ti.end_date = datetime.now()
            session.merge(ti)

        logging.info("Marking tasks as succeded: " + str(success_list))
        for task in success_list:
            ti = TaskInstance(
                task, execution_date=context['ti'].execution_date)
            ti.state = State.SUCCESS
            ti.start_date = datetime.now()
            ti.end_date = datetime.now()
            session.merge(ti)

        session.commit()
        session.close()
        logging.info("Done.")
