import logging
from mrjob.util import log_to_stream

__author__ = 'Felix'

import os
import subprocess
import sys

from mrjob.job import MRJob

from stats import PostJobHandler, PrintRecorder
from protocol import HBaseProtocol, TsvProtocol

from inspect import isclass
from distcache import *

HADOOP_JAR_HOME = '/usr/lib/hadoop-0.20-mapreduce'

std_run_modes = ['local', 'emr', 'hadoop', 'inline']
std_hadoop_home = '/usr/bin/hadoop'

lib_path = os.path.abspath(
    os.path.join(os.path.join(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'), '..'), 'pygz'))
lib_file = 'pylib.tar.gz'

DFS_BLOCK_SIZE = 128


def get_zookeeper_host():
    from lxml.etree import XML
    xml_data = file('/etc/hbase/conf/hbase-site.xml', 'rb').read()
    for host in XML(xml_data).xpath("//property[name='hbase.zookeeper.quorum']/value")[0].text.split(','):
        return host


def get_region_servers():
    from kazoo.client import KazooClient
    zk = KazooClient(hosts=get_zookeeper_host(), read_only=True)
    try:
        zk.start()
        children = zk.get_children('/hbase/rs')
        return [c.encode('utf-8').split(',')[0] for c in children]
    finally:
        zk.stop()


class JobBuilder:

    GZ_COUNTER = 0

    max_map_fail_percentage = 90
    max_map_task_fails = 4
    max_reduce_task_fails = 8

    def __init__(self, job_name='MRJob'):
        os.environ['HADOOP_LOG_DIR'] = '/user/felixv/logs'
        self.stages = []
        self.args = [
            '--no-output',
            '--strict-protocols',
            '--cleanup', 'NONE',
            '--python-archive', '%s/%s' % (lib_path, lib_file),
            '--jobconf', ('mapred.job.name=%s' % job_name),
            '--jobconf', ('mapred.max.map.failures.percent=%d' % self.max_map_fail_percentage),
            '--jobconf', ('mapred.map.max.attempts=%d' % self.max_map_task_fails),
            '--jobconf', ('mapred.reduce.max.attempts=%d' % self.max_reduce_task_fails),
            '--setup', 'export PATH=$PATH:/usr/lib/python2.6/site-packages:/usr/lib64/python2.6/site-packages',
            '--setup', 'export PYTHONPATH=$PYTHONPATH:$PATH'
        ]

        self.input_type = 'plain'
        self.combine_input = False

        self.input_paths = []
        self.output_method = 'file'
        self.output_path = None
        self.deleted_paths = []

        self.setups = []
        self.follow_ups = []

        self.add_follow_up(PostJobHandler([PrintRecorder()]).handle_job)

    def with_avro_input(self):
        # refactor laster to add jars normally
        self.input_type = 'avro'
        self.args += ['--hadoop-arg', '-libjars']
        self.args += ['--hadoop-arg', '%s/lib/avro.jar,%s/avro-mapred-1.7.3-hadoop2.jar' % (HADOOP_JAR_HOME, lib_path)]
        return self

    def with_sequence_file_input(self):
        self.input_type = 'sequence'
        return self


    def combine_input_files(self, chunk=None):
        self.combine_input = True
        if chunk is not None:
            self.combine_chunk = chunk

        self.args += ['--hadoop-arg', '-libjars']
        self.args += ['--hadoop-arg', '%s/common.jar' % lib_path]
        return self    

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

    def output_to_hbase(self, table, cf=None, servers=None):
        self.output_method = 'hbase'
        self.args += ['--setup', 'export %s=%s' % (HBaseProtocol.HBASE_TABLE_ENV, table)]
        if cf:
            self.args += ['--setup', 'export %s=%s' % (HBaseProtocol.HBASE_COLUMN_FAMILY_ENV, cf)]

        if servers is None:
            servers = get_region_servers()
            sys.stderr.write('region servers: %s\n' % str(servers))

        self.args += ['--setup', 'export %s=%s' % (HBaseProtocol.HBASE_SERVER_ENV, ','.join(servers))]
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
            self.args += ['--jobconf', 'mapreduce.output.fileoutputformat.compress=true']
            self.args += ['--jobconf', 'mapreduce.output.fileoutputformat.compress.codec=%s' % codec_name]

        return self

    def with_input_split_size(self, megabytes):
        self.args += ['--jobconf', ('mapred.max.split.size=%d' % (megabytes * 1024 * 1024))]
        self.args += ['--jobconf', ('mapred.min.split.size=%d' % (megabytes * 1024 * 1024))]

        if megabytes < DFS_BLOCK_SIZE:
            self.args += ['--jobconf', ('dfs.blocksize=%d' % (megabytes * 1024 * 1024))]

        return self

    def with_task_memory(self, megabytes, task_type='all'):
        if task_type == 'map' or task_type == 'all':
            self.args += ['--jobconf', 'mapreduce.map.memory.mb=%d' % (int)(megabytes * 1.3)]
            self.args += ['--jobconf', ('mapreduce.map.java.opts=-Xmx%(mems)dm -Xms%(mems)dm' % {'mems': megabytes})]

        if task_type == 'reduce' or task_type == 'all':
            self.args += ['--jobconf', 'mapreduce.reduce.memory.mb=%d' % (int)(megabytes * 1.3)]
            self.args += ['--jobconf', ('mapreduce.reduce.java.opts=-Xmx%(mems)dm -Xms%(mems)dm' % {'mems': megabytes})]

        return self

    def with_io_memory(self, megabytes, task_type='all'):
        self.args += ['--jobconf', ('io.sort.mb=%d' % megabytes)]
        return self

    def pool(self, pool):
        self.args += ['--jobconf', ('mapred.fairscheduler.pool=%s' % pool)]
        return self

    def num_reducers(self, reducers):
        self.args += ['--jobconf', ('mapred.reduce.tasks=%s' % reducers)]
        return self

    def cache_files_keyed(self, key, path):
        files_to_cache = find_files(path)

        for cache_file in files_to_cache:
            self.args += ['--file', 'hdfs://%s#%s' % (cache_file, '%s_%s' % (key, cache_file.split('/')[-1:][0]))]

        self.args += ['--setup', cache_files_cmd(files_to_cache, key)]
        return self

    def cache_files(self, path):
        return self.cache_files_keyed('', path)

    def cache_object_keyed(self, key, obj):
        obj_file, key_cmd = cache_obj(key, obj)
        self.args += ['--file', obj_file]
        self.args += ['--setup', key_cmd]
        self.add_follow_up_cmd('rm %s' % obj_file)
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

    def include_all_files(self, files):
        for file in files:
            self.include_file(file)

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

    def include_all_dirs(self, dirs):
        for dir in dirs:
            self.include_dir(dir)

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
                          '%s/contrib/streaming/hadoop-streaming.jar' % HADOOP_JAR_HOME]

            log_dir = None

            if self.output_method == 'file':
                self.args += ['--output-dir', ('hdfs://%s' % self.output_path)]
                log_dir = '%s/_logs/history/' % self.output_path

            for path in self.deleted_paths:
                self.add_setup_cmd('hadoop fs -rm -r -f %s' % path)

            self.args += [('hdfs://%s' % path) for path in self.input_paths]
        else:
            for path in self.deleted_paths:
                self.add_setup_cmd('rm -rf %s' % path)

            if self.output_method == 'file':
                self.args += ['--output-dir', self.output_path]

            self.args += self.input_paths

        for setup in self.setups:
            setup()

        job = job_cls(self.args)

        if self.input_type == 'avro':
            job.HADOOP_INPUT_FORMAT = 'org.apache.avro.mapred.AvroAsTextInputFormat'
        elif self.input_type == 'sequence':
            job.HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.SequenceFileAsTextInputFormat'
        elif self.combine_input:
            job.HADOOP_INPUT_FORMAT = 'com.similargroup.common.combine.CombineTextInputFormat'

        job.follow_ups = self.follow_ups

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

