from airflow import settings, utils
from airflow.models import TaskInstance, Log
from airflow.operators.python_operator import PythonOperator
from airflow.plugins_manager import AirflowPlugin
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
--sig-proxy=false                                             \
--user=`id -u`                                                \
-e DOCKER_GATE={{ docker_manager }}                           \
-e GELF_HOST="runsrv2.sg.internal"                            \
-e HOME=/tmp                                                  \
runsrv/%(docker)s bash -c "sudo mkdir -p {{ params.execution_dir }} && sudo cp -r /tmp/dockexec/%(random)s/* {{ params.execution_dir }} && %(bash_command)s"
    '''

    @apply_defaults
    def __init__(self, docker_name, bash_command, *args, **kwargs):
        self.docker_name = docker_name
        random_string = str(datetime.utcnow().strftime('%s'))
        docker_command = DockerBashOperator.cmd_template % {'random': random_string, 'docker': self.docker_name,
                                                            'bash_command': bash_command}
        super(DockerBashOperator, self).__init__(bash_command=docker_command, *args, **kwargs)


class DockerBashSensor(BashSensor):
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
--sig-proxy=false                                             \
--user=`id -u`                                                \
-e DOCKER_GATE={{ docker_manager }}                           \
-e GELF_HOST="runsrv2.sg.internal"                            \
-e HOME=/tmp                                                  \
runsrv/%(docker)s bash -c "sudo mkdir -p {{ params.execution_dir }} && sudo cp -r /tmp/dockexec/%(random)s/* {{ params.execution_dir }} && %(bash_command)s"
    '''

    @apply_defaults
    def __init__(self, docker_name, bash_command, *args, **kwargs):
        self.docker_name = docker_name
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
        # Add echo to everything if we have dryrun in request
        if self.dag.params and '--dryrun' in self.dag.params.get('transients', ''):
            logging.info("Dry rub requested. Don't really copy table")
            self.bash_command = '\n'.join(['echo ' + line for line in self.bash_command.splitlines()])


class DockerCopyHbaseTableOperator(BashOperator):
    ui_color = '#0099FF'
    cmd_template = '''source {{ params.execution_dir }}/scripts/infra.sh
hbasecopy %(source_cluster)s %(target_cluster)s %(table_name)s
    '''

    template_fields = ('bash_command', 'docker_name')
    dock_cmd_template = '''docker -H=tcp://{{ params.docker_gate }}:2375 run       \
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
-e DOCKER_GATE={{ params.docker_gate }}                           \
-e GELF_HOST="runsrv2.sg.internal"                            \
-e HOME=/tmp                                                  \
runsrv/%(docker)s bash -c "sudo mkdir -p {{ params.execution_dir }} && sudo cp -r /tmp/dockexec/%(random)s/* {{ params.execution_dir }} && %(bash_command)s"
    '''

    @apply_defaults
    def __init__(self, docker_name, source_cluster, target_cluster, table_name_template, *args, **kwargs):
        self.docker_name = docker_name
        bash_cmd = DockerCopyHbaseTableOperator.cmd_template % {'source_cluster': source_cluster,
                                                                'target_cluster': target_cluster,
                                                                'table_name': table_name_template}
        random_string = str(datetime.utcnow().strftime('%s'))
        docker_command = DockerCopyHbaseTableOperator.dock_cmd_template % {'random': random_string,
                                                                           'docker': docker_name,
                                                                           'bash_command': bash_cmd}
        super(DockerCopyHbaseTableOperator, self).__init__(bash_command=docker_command, *args, **kwargs)

        # Add echo to everything if we have dryrun in request
        if self.dag.params and '--dryrun' in self.dag.params.get('transients', ''):
            logging.info("Dry rub requested. Don't really copy table")
            self.bash_command = '\n'.join(['echo ' + line for line in self.bash_command.splitlines()])


class SuccedOrSkipOperator(PythonOperator):
    ui_color = '#CC6699'

    def execute(self, context):
        logging.info('Task params: ' + str((self.op_args, self.op_kwargs)))
        skip_list, success_list = super(SuccedOrSkipOperator, self).execute(context)

        logging.info("Skipping taks: " + str(skip_list))
        logging.info("Marking success for: " + str(success_list))
        session = settings.Session()

        for task in self.get_flat_relatives():
            if task.task_id not in (skip_list + success_list):
                continue
            ti = TaskInstance(
                task, execution_date=context['ti'].execution_date)
            ti.start_date = datetime.now()
            ti.end_date = datetime.now()
            if task.task_id in skip_list:
                ti.state = State.SKIPPED
                session.add(Log(State.SKIPPED, ti))
            else:
                ti.state = State.SUCCESS
                session.add(Log(State.SUCCESS, ti))
            session.merge(ti)

        session.commit()
        session.close()
        if self.task_id not in success_list:
            raise ValueError(
                "Skipped this, so we don't want to succed in this task")  # Need to throw an exception otherwise task will succeed
        logging.info("Done.")

    def run(self, start_date=None, end_date=None, ignore_dependencies=False, force=False, mark_success=False):
        """
        Run a set of task instances for a date range.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date or datetime.now()

        # We mark our own successes if needed. Run in "test" mode
        for dt in utils.date_range(start_date, end_date, self.schedule_interval):
            TaskInstance(self, dt).run(
                mark_success=False,
                ignore_dependencies=ignore_dependencies,
                test_mode=True,
                force=force, )


class SWAAirflowPluginManager(AirflowPlugin):
    name = 'SWOperators'

    operators = [BashSensor, DockerBashOperator, DockerBashSensor, CopyHbaseTableOperator, SuccedOrSkipOperator]
