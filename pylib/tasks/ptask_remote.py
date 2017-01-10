__author__ = 'Felix'

from datetime import datetime
import random

python_bin = '/opt/anaconda/envs/mrp27/bin/python'
ptask_invoker = 'pylib/python/pylib/tasks/ptask_invoke.py'


def random_str(length):
    chars = [chr(ord('a') + x) for x in range(ord('z') - ord('a'))]
    return ''.join(random.choice(chars) for _ in range(length))


class DockerInvoker:

    dock_cmd_template = '''docker \
-H=tcp://%(docker_gate)s \
run \
-v %(execution_dir)s:/tmp/dockexec/mapped_code \
-v /etc/localtime:/etc/localtime:ro \
-v /tmp/logs:/tmp/logs \
-v /var/lib/sss:/var/lib/sss \
-v /etc/passwd:/etc/passwd \
-v /etc/localtime:/etc/localtime:ro \
-v /usr/bin:/opt/old_bin \
-v /var/run/similargroup:/var/run/similargroup \
--rm \
--name=%(container_name)s \
--sig-proxy=false \
--user=%(user)s \
-e DOCKER_GATE=%(docker_gate)s \
-e GELF_HOST="runsrv2.sg.internal" \
-e HOME=/tmp \
%(docker_repository)s/centos6.cdh5.%(docker)s bash -c " \
sudo mkdir -p %(execution_dir)s && \
sudo cp -r /tmp/dockexec/mapped_code/* %(execution_dir)s && \
%(bash_command)s"
    '''

    def __init__(self, user='jupyter', execution_dir='/similargroup/production', gate='docker-a02.sg.internal:2375', repos='docker.similarweb.io:5000/bigdata', image='mrp'):
        self.user = user
        self.execution_dir = execution_dir
        self.docker_gate = gate
        self.docker_repository = repos
        self.image = image

    def run_task(self, collection, task, date, mode, mode_type, input_base='/similargroup/data', output_base='/similargroup/data', table_prefix='', dry_run=False, task_id=None, **kwargs):
        last_slash_ind = collection.rfind('/')
        if last_slash_ind == -1:
            # collection_name = collection
            collection_sub_path = ''
        else:
            collection_sub_path = collection[:last_slash_ind]
            # collection_name = collection[last_slash_ind + 1:]

        ptask_cmd = 'ptask --root %(root)s/%(collection_sub_path)s -c /%(collection)s --dt %(date)s --mode %(mode)s --mode-type %(mt)s \
                     --base-dir %(base_dir)s --calc-dir %(calc_dir)s %(dry_run_opt)s %(tp_opt)s %(task)s %(extra_opts)s' % \
                    {
                        'python': python_bin,
                        'root': self.execution_dir,
                        'invoke': ptask_invoker,
                        'collection': collection,
                        'collection_sub_path': collection_sub_path,
                        'date': date.strftime('%Y-%m-%d') if isinstance(date, datetime) else str(date),
                        'mode': mode,
                        'mt': mode_type,
                        'base_dir': input_base,
                        'calc_dir': output_base,
                        'dry_run_opt': '--dr' if dry_run else '',
                        'tp_opt': ('--tp %s' % table_prefix) if (table_prefix is not None and table_prefix != '') else '',
                        'extra_opts': ' '.join(['--%s %s' % (param.replace('_', '-'), str(value)) for (param, value) in kwargs.items()]),
                        'task': task
                    }

        self._run_command(ptask_cmd, task_id or random_str(6))

    def _run_command(self, cmd, task_id):
        import subprocess
        params = {
            'docker_gate': self.docker_gate,
            'execution_dir': self.execution_dir,
            'user': self.user,
            'container_name': 'remote_ptask_%s' % task_id,
            'docker_repository': self.docker_repository,
            'docker': self.image,
            'bash_command': cmd
        }

        final_cmd = DockerInvoker.dock_cmd_template % params
        print(final_cmd)
        subprocess.check_call(['bash', '-c', final_cmd])

