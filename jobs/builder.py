__author__ = 'Felix'

import os
import subprocess

from mrjob.job import MRJob

from stats import PostJobHandler, PrintRecorder
from protocol import HBaseProtocol

std_run_modes = ['local', 'emr', 'hadoop', 'inline']
std_hadoop_home = '/usr/bin/hadoop'

lib_path = os.path.abspath(os.path.join(os.path.join(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'), '..'), 'pylib'))
lib_file = 'jobs.tar.gz'


class JobBuilder:

    max_map_fails = 90
    max_reduce_task_fails = 20

    def __init__(self, job_name='MRJob'):
        self.stages = []
        self.args = [
                     '--no-output',
                     '--python-archive', '%s/%s' % (lib_path, lib_file),
                     '--jobconf', ('mapred.job.name=%s' % job_name),
                     '--jobconf', ('mapred.max.map.failures.percent=%d' % self.max_map_fails),
                     '--jobconf', ('mapred.reduce.max.attempts=%d' % self.max_reduce_task_fails),
                     '--setup', 'export PATH=$PATH:/usr/lib/python2.6/site-packages:/usr/lib64/python2.6/site-packages',
                     '--setup', 'export PYTHONPATH=$PYTHONPATH:$PATH'
                     ]

        self.input_paths = []
        self.output_method = 'file'
        self.output_path = None
        self.deleted_paths = []

        self.setups = []
        self.follow_ups = []

        self.add_follow_up(PostJobHandler([PrintRecorder()]).handle_job)


    def add_input_path(self, input_path, combine=False):
        self.input_paths += [input_path]
        return self

    def output_on(self, output_path):
        self.output_method = 'file'
        self.output_path = output_path
        return self

    def delete_output_on_start(self):
        if self.output_path:
            self.deleted_paths += [self.output_path]

        return self

    def delete_on_start(self, path):
        self.deleted_paths += [path]
        return self

    def output_to_hbase(self, table, cf=None):
        self.output_method = 'hbase'
        self.args += ['--setup', 'export %s=%s' % (HBaseProtocol.HBASE_TABLE_ENV, table)]
        if cf:
            self.args += ['--setup', 'export %s=%s' % (HBaseProtocol.HBASE_COLUMN_FAMILY_ENV, cf)]

        return self

    def pool(self, pool):
        self.args += ['--jobconf', ('mapred.fairscheduler.pool=%s' % pool)]
        return self

    def num_reducers(self, reducers):
        self.args += ['--jobconf', ('mapred.reduce.tasks=%s' % reducers)]
        return self

    def add_setup(self, setup):
        self.setups += [setup]
        return self

    def add_setup_cmd(self, cmd_str):
        def cmd(): subprocess.Popen(cmd_str.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.add_setup(cmd)
        return self

    def add_follow_up(self, follow_up):
        self.follow_ups += [follow_up]
        return self

    def add_follow_up_cmd(self, cmd_str):
        def cmd(): subprocess.Popen(cmd_str.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.add_follow_up(cmd)
        return self

    def include_file(self, file):
        self.args += ['--file', file]
        return self

    def set_property(self, prop_name, prop_value):
        self.args += [('--%s' % prop_name), prop_value]
        return self

    # validation checks for
    def do_checks(self):
        if len(self.input_paths) == 0:
            return False, 'input path not configured'
        if self.output_method != 'hbase' and not self.output_path:
            return False, 'output path not configured'

        return True, 'OK'

    def get_job(self, job_cls, runner='hadoop', **kwargs):

        check, msg = self.do_checks()
        if not check:
            raise Exception('Invalid job configuration: %s', msg)

        if runner and runner in std_run_modes:
            self.args += ['-r', runner]

        if runner == 'hadoop' or runner == 'dry':

            hadoop_home = kwargs['hadoop_home'] if 'hadoop_home' in kwargs else std_hadoop_home
            os.environ['HADOOP_HOME'] = hadoop_home
            self.args += ['--hadoop-bin', hadoop_home]
            self.args += ['--hadoop-streaming-jar', '/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar']

            if self.output_method == 'file':
                self.args += ['--output-dir', ('hdfs://%s' % self.output_path)]

            self.args += [('hdfs://%s' % path) for path in self.input_paths]
        else:
            if self.output_method == 'file':
                self.args += ['--output-dir', self.output_path]

            self.args += self.input_paths

        for setup in self.setups:
            setup()

        job = job_cls(self.args)

        job.follow_ups = self.follow_ups

        return job


class Job(MRJob):

    follow_ups = []

    def post_exec(self, **kwargs):
        for follow_up in self.follow_ups:
            follow_up(**kwargs)


if __name__ == '__main__':
    print 'do not use this as main'

