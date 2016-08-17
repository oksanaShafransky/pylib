import re
from datetime import datetime
from dateutil import parser
import subprocess
from copy import copy

import common
from common import GracefulShutdownHandler


class HiveParamBuilder:

    TASK_MEMORY_OVERHEAD = 0.3

    COMPRESSION_FORMATS = {
        'gz': 'org.apache.hadoop.io.compress.GzipCodec',
        'bz2': 'org.apache.hadoop.io.compress.BZip2Codec'
    }

    def __init__(self):
        self.root_queue = None
        self.pool = 'calculation'
        self.slow_start = 1.0

        self.map_cpu_cores = 2
        self.map_task_memory = 1024  # MB
        self.input_block_size = 128

        self.reduce_cpu_cores = 2
        self.reduce_task_memory = 2048

        self.compression = None
        self.consolidate = True

        self.child_opts = {
            'all': set(),
            'map': set(),
            'reduce': set()
        }

    def add_child_option(self, option, where='all', condition=True):
        if condition:
            self.child_opts[where].add(option)
        return self

    def with_memory(self, amount_in_mb, where='all', condition=True):
        if condition:
            if where == 'all':
                self.map_task_memory = amount_in_mb
                self.reduce_task_memory = amount_in_mb
            elif where == 'map':
                self.map_task_memory = amount_in_mb
            elif where == 'reduce':
                self.reduce_task_memory = amount_in_mb
            else:
                raise ValueError('target must be either all, map or reduce')

        return self

    def with_cores(self, amount, where='all', condition=True):
        if condition:
            if where == 'all':
                self.map_cpu_cores = amount
                self.reduce_task_memory = amount
            elif where == 'map':
                self.map_cpu_cores = amount
            elif where == 'reduce':
                self.reduce_cpu_cores = amount
            else:
                raise ValueError('target must be either all, map or reduce')

        return self

    def set_pool(self, pool, condition=True):
        if condition:
            self.pool = pool
        return self

    def set_queue(self, queue, condition=True):
        if condition:
            self.root_queue = queue
        return self

    def as_rerun(self, condition=True):
        if condition:
            return self.set_queue('reruns')

    def start_reduce_early(self, map_percentage, condition=True):
        if condition:
            self.slow_start = map_percentage
        return self

    def with_input_block_size(self, size, condition=True):
        if condition:
            self.input_block_size = size
        return self

    def set_compression(self, compression, condition=True):
        if condition:
            self.compression = compression if compression != 'none' else None
        return self

    def consolidate_output(self, whether_to_consolidate, condition=True):
        if condition:
            self.consolidate = whether_to_consolidate
        return self

    def to_conf(self):
        self.add_child_option('-Xmx%(memory)dm -Xms%(memory)dm' % {'memory': self.map_task_memory * (1 + HiveParamBuilder.TASK_MEMORY_OVERHEAD)}, where='map')
        self.add_child_option('-Xmx%(memory)dm -Xms%(memory)dm' % {'memory': self.reduce_task_memory * (1 + HiveParamBuilder.TASK_MEMORY_OVERHEAD)}, where='reduce')

        ret = {
            'mapreduce.job.queuename': ('%s.%s' % (self.root_queue, self.pool)) if self.root_queue is not None else self.pool,
            'mapreduce.input.fileinputformat.split.maxsize': self.input_block_size * 1024 * 1024,
            'mapreduce.map.cpu.vcores': self.map_cpu_cores,
            'mapreduce.reduce.cpu.vcores': self.reduce_cpu_cores,
            'mapreduce.task.io.sort.mb': max(self.map_task_memory / 10, 256),
            'hive.merge.mapredfiles': 'true' if self.consolidate else 'false'
        }

        if len(self.child_opts['all']) > 0:
            ret['mapreduce.child.java.opts'] = ' '.join(self.child_opts['all'])

        if len(self.child_opts['map']) > 0:
            ret['mapreduce.map.java.opts'] = ' '.join(self.child_opts['map'])

        if len(self.child_opts['reduce']) > 0:
            ret['mapreduce.reduce.java.opts'] = ' '.join(self.child_opts['reduce'])

        ret['hive.exec.compress.output'] = 'true' if self.compression is not None else 'false'
        if self.compression is not None:
            ret['mapreduce.output.fileoutputformat.compress.codec'] = HiveParamBuilder.COMPRESSION_FORMATS[self.compression]

        return ret


class HiveProcessRunner:

    DEFAULT_HIVE_CONFIG = {
        'io.seqfile.compression': 'BLOCK',
        'hive.hadoop.supports.splittable.combineinputformat': 'true',
        'hive.exec.max.dynamic.partitions': 100000,
        'hive.exec.max.dynamic.partitions.pernode': 100000,
        'hive.exec.scratchdir': '/tmp/hive-prod',
        'hive.vectorized.execution.enabled': 'true',
        'hive.vectorized.execution.reduce.enabled': 'true',
        'hive.cbo.enable': 'true',
        'hive.stats.fetch.column.stats': 'true'
    }

    def __init__(self):
        pass

    @staticmethod
    def find_jobs(log_path, start_dt=None, end_dt=None):
        jobs = set()
        for line in open(log_path, 'rb'):
            ts_fields = line.strip().split(' ')[:2]
            try:
                ts = parser.parse(' '.join(ts_fields).replace(',', '.'), yearfirst=True, dayfirst=False)
            except ValueError:
                continue  # a non ordinary line

            if (start_dt is not None and ts < start_dt) or (end_dt is not None and ts > end_dt):
                continue
            for job_id in re.findall('job_\d+_\d+', line):
                jobs.add(job_id)

        return list(jobs)

    def _run_hive(self, cmd, log_path):
        start_time = datetime.now()

        def kill_jobs():
            for job_id in self.find_jobs(log_path, start_time):
                common.logger.info('Killing job: %s' % job_id)
                kill_cmd = 'hadoop job -kill %s' % job_id
                kill_p = subprocess.Popen(kill_cmd)
                kill_p.wait()

        p = subprocess.Popen(cmd)
        with GracefulShutdownHandler(kill_jobs):
            p.wait()
            end_time = datetime.now()

        jobs = self.find_jobs(log_path, start_time)
        print 'finished running %d job(s) after %s with ret code %d' % (len(jobs), end_time - start_time, p.returncode)
        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, cmd)

    def run_query(self, hql, hive_params, job_name=None, partitions=None, log_dir='/tmp/logs', is_dry_run=False, **extra_hive_confs):
        params = copy(HiveProcessRunner.DEFAULT_HIVE_CONFIG)
        params.update(hive_params.to_conf())
        params.update(extra_hive_confs)

        if job_name is not None:
            params['mapreduce.job.name'] = job_name

        if partitions is not None:
            params['mapreduce.job.reduces'] = partitions

        params['hive.log.dir'] = log_dir
        params['hive.log.file'] = 'hive.log'

        hive_cmd = ['hive', '-e', hql]
        for param, val in params.iteritems():
            hive_cmd += ['-hiveconf', '%s=%s' % (param, str(val))]

        common.logger.info('CMD:\n%s' % ' '.join(hive_cmd))
        if not is_dry_run:
            return self._run_hive(hive_cmd, log_path='%s/hive.log' % log_dir)
        else:
            common.logger.info('Not executing, this is just a dry run')
