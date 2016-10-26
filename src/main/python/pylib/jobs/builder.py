import logging
import subprocess
import sys
from inspect import isclass

from mrjob.job import MRJob
from mrjob.util import log_to_stream

from distcache import *
from protocol import HBaseProtocol, TsvProtocol
from stats import PostJobHandler, PrintRecorder

__author__ = 'Felix'

HADOOP_JAR_HOME = '/usr/lib/hadoop-0.20-mapreduce'
PYTHON_HOME = '/opt/anaconda/envs/mrp27/bin/python'

std_run_modes = ['local', 'emr', 'hadoop', 'inline']
std_hadoop_home = '/usr/bin/hadoop'

user_path = 'USER'
yarn_queue_param = 'JOB_QUEUE'

lib_path = '~'
lib_file = 'installed_pylib_src.zip'

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


class JobBuilder(object):
    GZ_COUNTER = 0

    max_map_fail_percentage = 0
    max_map_task_fails = 4
    max_reduce_task_fails = 8

    def __init__(self, job_name='MRJob'):
        os.environ['HADOOP_LOG_DIR'] = '/user/felixv/logs'
        self.stages = []
        self.map_java_opts = []
        self.reduce_java_opts = []
        self.args = [
            '--no-output',
            '--strict-protocols',
            '--cleanup', 'NONE',
            '--archive', '%s/%s#%s' % (lib_path, lib_file, lib_file),
            '--setup', 'export PYTHONPATH=$PYTHONPATH:./%s/%s' % (lib_file, lib_file[:lib_file.rfind('.')]),
            '--jobconf', ('mapreduce.job.name=%s' % job_name),
            '--jobconf', ('mapreduce.map.failures.maxpercent=%d' % self.max_map_fail_percentage),
            '--jobconf', ('mapreduce.map.maxattempts=%d' % self.max_map_task_fails),
            '--jobconf', ('mapreduce.reduce.maxattempts=%d' % self.max_reduce_task_fails),
            '--python-bin', PYTHON_HOME
        ]

        self.input_type = 'plain'

        self.input_paths = []
        self.output_method = 'file'
        self.output_path = None
        self.deleted_paths = []

        self.setups = []
        self.follow_ups = []

        self.add_follow_up(PostJobHandler([PrintRecorder()]).handle_job)

    def with_combined_text_input(self, split_size=128 * 1024 * 1024):
        self.args += ['--hadoop-arg', '-libjars']
        self.args += ['--hadoop-arg', '/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-core.jar']
        self.args += ['--jobconf', ('mapreduce.input.fileinputformat.split.maxsize=%d' % split_size)]

        self.input_type = 'text_combined'
        return self

    def with_avro_input(self):
        self.input_type = 'avro'
        self.args += ['--hadoop-arg', '-libjars']
        self.args += ['--hadoop-arg', '/usr/lib/avro/avro-mapred-hadoop2.jar']
        return self

    def with_sequence_file_input(self):
        # self.args += ['-hadoop_input_format', 'org.apache.avro.mapred.AvroAsTextInputFormat']
        # refactor laster to add jars normally
        core_lib_path = '/usr/lib/hadoop-0.20-mapreduce/hadoop-core-mr1.jar'
        if not os.path.exists(core_lib_path):
            core_lib_path = '/usr/lib/hadoop-0.20-mapreduce/hadoop-core.jar'
        self.args += ['--hadoop-arg', '-libjars']
        self.args += ['--hadoop-arg', core_lib_path]
        self.input_type = 'sequence'
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
        self.args += ['--jobconf',
                      ('mapreduce.partition.keypartitioner.options=-k%d,%d' % (key_part_start, key_part_end))]

        return self

    def compress_output(self, codec='gz'):
        if codec == 'gz':
            codec_name = 'org.apache.hadoop.io.compress.GzipCodec'
        elif codec == 'bz2':
            codec_name = 'org.apache.hadoop.io.compress.BZip2Codec'
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

    def with_task_cores(self, cores, task_type='all'):
        if task_type == 'all' or task_type == 'map':
            self.args += ['--jobconf', 'mapreduce.map.cpu.vcores=%d' % cores]

        if task_type == 'all' or task_type == 'reduce':
            self.args += ['--jobconf', 'mapreduce.reduce.cpu.vcores=%d' % cores]

        return self

    def with_map_java_opts(self, child_java_opts):
        self.map_java_opts += [child_java_opts]

        return self

    def with_reduce_java_opts(self, child_java_opts):
        self.reduce_java_opts += [child_java_opts]

        return self

    def with_map_profiler(self):
        self.map_java_opts += ['-agentpath:/opt/yjp/bin/libyjpagent.so']

        return self

    def with_reduce_profiler(self):
        self.reduce_java_opts += ['-agentpath:/opt/yjp/bin/libyjpagent.so']

        return self

    def with_task_memory(self, megabytes, task_type='all'):
        if task_type == 'all' or task_type == 'map':
            self.map_java_opts += ['-Xmx%(mems)dm -Xms%(mems)dm' % {'mems': megabytes}]
            self.args += ['--jobconf', 'mapreduce.map.memory.mb=%d' % megabytes]

        if task_type == 'all' or task_type == 'reduce':
            self.reduce_java_opts += ['-Xmx%(mems)dm -Xms%(mems)dm' % {'mems': megabytes}]
            self.args += ['--jobconf', 'mapreduce.reduce.memory.mb=%d' % megabytes]

        return self

    def with_io_memory(self, megabytes, task_type='all'):
        self.args += ['--jobconf', ('io.sort.mb=%d' % megabytes)]
        return self

    def pool(self, pool):
        job_queue = '%s.%s' % (os.environ[yarn_queue_param], pool) if yarn_queue_param in os.environ else pool
        self.args += ['--jobconf', ('mapreduce.job.queuename=%s' % job_queue)]
        return self

    def num_reducers(self, reducers):
        self.args += ['--jobconf', ('mapreduce.job.reduces=%s' % reducers)]
        return self

    def cache_files_keyed(self, key, path):
        cache_dir, files_to_cache = find_files(path, prefix='%s_' % key)
        self.args += ['--file', 'hdfs://%s#%s' % (path, cache_dir)]
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

    def include_file(self, file_name):
        self.args += ['--file', file_name]
        return self

    def include_all_files(self, files):
        for current_file in files:
            self.include_file(current_file)

        return self

    def get_next_gz(self):
        ret = '/tmp/%d.tar.gz' % JobBuilder.GZ_COUNTER
        JobBuilder.GZ_COUNTER += 1
        return ret

    def include_dir(self, dir_path):
        archive_name = self.get_next_gz()
        self.add_setup_cmd('tar -zcvf %s %s' % (archive_name, dir_path))
        self.args += ['--python-archive', '%s' % archive_name]
        self.add_follow_up_cmd('rm %s' % archive_name)
        return self

    def include_all_dirs(self, dirs):
        for dir_path in dirs:
            self.include_dir(dir_path)

        return self

    def set_property(self, prop_name, prop_value, condition=True):
        if condition:
            self.args += [('--%s' % prop_name), prop_value]
        return self

    def set_job_conf(self, key, value):
        self.args += ['--jobconf', ('%s=%s' % (key, value))]
        return self

    # validation checks for
    def do_checks(self):
        if len(self.input_paths) == 0:
            return False, 'input path not configured'
        if self.output_method != 'hbase' and not self.output_path:
            return False, 'output path not configured'

        return True, 'OK'

    def get_job(self, job_cls, runner='hadoop', **kwargs):
        if self.map_java_opts:
            self.args += ['--jobconf', ('mapreduce.map.java.opts=%s' % ' '.join(self.map_java_opts))]
        if self.reduce_java_opts:
            self.args += ['--jobconf', ('mapreduce.reduce.java.opts=%s' % ' '.join(self.reduce_java_opts))]

        check, msg = self.do_checks()
        if not check:
            raise Exception('Invalid job configuration: %s', msg)

        if runner and runner in std_run_modes:
            self.args += ['-r', runner]

        if runner == 'hadoop' or runner == 'dry':
            # Streaming jars are named differently in different CDH versions
            streaming_jar_path = '%s/contrib/streaming/hadoop-streaming-mr1.jar' % HADOOP_JAR_HOME
            if not os.path.exists(streaming_jar_path):
                streaming_jar_path = '%s/contrib/streaming/hadoop-streaming.jar' % HADOOP_JAR_HOME

            hadoop_home = kwargs['hadoop_home'] if 'hadoop_home' in kwargs else std_hadoop_home
            os.environ['HADOOP_HOME'] = hadoop_home
            self.args += ['--hadoop-bin', hadoop_home]
            self.args += ['--hadoop-streaming-jar',
                          streaming_jar_path]

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

        if self.input_type == 'text_combined':
            job.HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.CombineTextInputFormat'
        elif self.input_type == 'avro':
            job.HADOOP_INPUT_FORMAT = 'org.apache.avro.mapred.AvroAsTextInputFormat'
        elif self.input_type == 'sequence':
            job.HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.SequenceFileAsTextInputFormat'

        job.log_dir = None
        job.follow_ups = []
        # doesnt work right now with cdh 5 job.log_dir = log_dir
        # job.follow_ups = self.follow_ups

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
    print('do not use this as main')
