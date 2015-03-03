import logging
import tempfile
from mrjob.util import log_to_stream
from jobs.hbase import Exporter

__author__ = 'Felix'

import os
import subprocess

from mrjob.job import MRJob

from stats import PostJobHandler, PrintRecorder
from protocol import HBaseProtocol, TsvProtocol

from inspect import isclass
import cPickle as pickle
import xml.etree.ElementTree as ET

std_run_modes = ['local', 'emr', 'hadoop', 'inline']
std_hadoop_home = '/usr/bin/hadoop'

user_path = 'USER'

lib_path = os.path.abspath(
    os.path.join(os.path.join(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'), '..'), 'pygz'))
lib_file = 'pylib.tar.gz'


def determine_hbase_servers():
    hbase_conf = os.environ['HBASE_CONF_DIR'] if 'HBASE_CONF_DIR' in os.environ else '/etc/hbase/conf'

    conf = ET.parse('%s/hbase-site.xml' % hbase_conf)
    root = conf.getroot()

    # should only be 1
    quorum_prop = [elem.find('value').text for elem in root.findall('property') if elem.find('name').text == 'hbase.zookeeper.quorum'][0]
    return quorum_prop.split(',')


class JobBuilder:

    GZ_COUNTER = 0

    max_map_fails = 90
    max_reduce_task_fails = 20

    def __init__(self, job_name='MRJob'):
        os.environ['HADOOP_LOG_DIR'] = '/user/felixv/logs'
        self.stages = []
        self.args = [
            '--no-output',
            '--strict-protocols',
            '--cleanup', 'NONE',
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

    def add_tsv_input_path(self, input_path, key_class, value_class, combine=False):
        if not isclass(key_class) or not hasattr(key_class, 'read_tsv'):
            raise Exception('key_class parameter must be a class with read_tsv method definition')
        if not isclass(value_class) or not hasattr(value_class, 'read_tsv'):
            raise Exception('value_class parameter must be a class with read_tsv method definition')
        self.args += ['--setup', 'export %s=%s.%s' % (
            TsvProtocol.named_key_class_env(input_path), key_class.__module__, key_class.__name__)]
        self.args += ['--setup', 'export %s=%s.%s' % (
            TsvProtocol.named_value_class_env(input_path), value_class.__module__, value_class.__name__)]

        return self.add_input_path(input_path, combine)

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

    def output_to_hbase(self, table, cf=None, server=None):
        self.output_method = 'hbase'
        self.args += ['--setup', 'export %s=%s' % (HBaseProtocol.HBASE_TABLE_ENV, table)]
        if cf:
            self.args += ['--setup', 'export %s=%s' % (HBaseProtocol.HBASE_COLUMN_FAMILY_ENV, cf)]

        if server is None:
            for server in determine_hbase_servers():
                try:
                    writer = Exporter(server, table, col_family=cf)
                    if writer:
                        self.args += ['--setup', 'export %s=%s' % (HBaseProtocol.HBASE_SERVER_ENV, server)]
                        break
                except:
                    continue
        return self

    def partition_by_key(self, key_part_start=1, key_part_end=1):
        self.args += ['--partitioner', 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner']
        self.args += ['--jobconf', ('mapreduce.partition.keypartitioner.options=-k%d,%d' % (key_part_start, key_part_end))]

        return self

    def compress_output(self, codec='gz'):
        if codec == 'gz':
            codec_name = 'org.apache.hadoop.io.compress.GzipCodec'
        elif codec == 'bz':
            codec_name = 'org.apache.hadoop.io.compress.BzipCodec'
        elif codec == 'snappy':
            codec_name = 'org.apache.hadoop.io.compress.SnappyCodec'
        else:
            codec_name = None

        if codec_name is not None:
            self.args += ['--jobconf', 'mapred.output.compress=true']
            self.args += ['--jobconf', 'mapred.output.compression.codec=%s' % codec_name]

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
        def cmd(**kwargs): subprocess.Popen(cmd_str.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        self.add_follow_up(cmd)
        return self

    def include_file(self, file):
        self.args += ['--file', file]
        return self

    def get_next_gz(self):
        ret = '/tmp/%d.tar.gz' % JobBuilder.GZ_COUNTER
        JobBuilder.GZ_COUNTER = JobBuilder.GZ_COUNTER + 1
        return ret

    def include_dir(self, dir):
        archive_name = self.get_next_gz()
        self.add_setup_cmd('tar -zcvf %s %s' % (archive_name, dir))
        self.args += ['--python-archive', '%s' % archive_name]
        self.add_follow_up_cmd('rm %s' % archive_name)
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
            self.args += ['--hadoop-streaming-jar',
                          '/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar']

            log_dir = None

            if self.output_method == 'file':
                self.args += ['--output-dir', ('hdfs://%s' % self.output_path)]
                log_dir = '%s/_logs/history/' % self.output_path
            else:
                log_dir = user_path

            for path in self.deleted_paths:
                self.add_setup_cmd('hadoop fs -rm -r -f %s' % path)

            self.args += [('hdfs://%s' % path) for path in self.input_paths]
        else:
            for path in self.deleted_paths:
                self.add_setup_cmd('rm -rf %s' % path)

            if self.output_method == 'file':
                self.args += ['--output-dir', self.output_path]

            self.args += self.input_paths

            log_dir = None

        for setup in self.setups:
            setup()

        job = job_cls(self.args)
        job.log_dir = None
        job.follow_ups = []
        # doesnt work right now with cdh 5 job.log_dir = log_dir
        #job.follow_ups = self.follow_ups

        return job


class Job(MRJob):
    follow_ups = []

    def __init__(self, args=None):
        super(Job, self).__init__(args)
        self._logger = None

    @property
    def logger(self):
        if self._logger is not None:
            return self._logger
        logger_name = type(self).__name__
        log_to_stream(logger_name)
        self._logger = logging.getLogger(logger_name)
        return self._logger


    def post_exec(self, **kwargs):
        for follow_up in self.follow_ups:
            follow_up(**kwargs)


if __name__ == '__main__':
    print 'do not use this as main'

