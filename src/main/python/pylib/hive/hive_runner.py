import os
import re
from datetime import datetime
from dateutil import parser
import subprocess

import common
from common import GracefulShutdownHandler
from pylib.jobs.builder import yarn_queue_param


class HiveProcessRunner:

    DEFAULT_TASK_MEMORY = 4096
    TASK_MEMORY_OVERHEAD = 0.3

    DEFAULT_INPUT_BLOCK_SIZE = 128

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

    def run_query(self, hql, hive_params, job_name=None, reducers=None, log_dir='/tmp/logs', **extra_hive_params):
        pass

    def run_hive_job(self, hql, job_name, num_of_reducers, log_dir, slow_start_ratio=None, calc_pool='calculation',
                     consolidate_output=True, compression='gz', task_memory=DEFAULT_TASK_MEMORY, input_block_size=None):
        if compression is None or compression == 'none':
            compress = 'false'
            codec = None
        elif compression == 'gz':
            compress = 'true'
            codec = "org.apache.hadoop.io.compress.GzipCodec"
        elif compression == "bz2":
            compress = 'true'
            codec = "org.apache.hadoop.io.compress.BZip2Codec"
        else:
            raise ValueError('Unknown compression type %s' % compression)

        effective_pool = (
            '%s.%s' % (os.environ[yarn_queue_param], calc_pool)) if yarn_queue_param in os.environ else calc_pool

        cmd = ["hive", "-e", '"%s"' % hql,
               "-hiveconf", "mapreduce.job.name=" + job_name,
               "-hiveconf", "mapreduce.job.reduces=" + str(num_of_reducers),
               "-hiveconf", "mapreduce.job.queuename=" + effective_pool,
               "-hiveconf", "hive.exec.compress.output=" + compress,
               "-hiveconf", "io.seqfile.compression=BLOCK",
               "-hiveconf", "hive.exec.max.dynamic.partitions=100000",
               "-hiveconf", 'hive.log.dir=%s' % log_dir,
               "-hiveconf", "hive.log.file=hive.log",
               "-hiveconf", "hive.exec.scratchdir=/tmp/hive-prod",
               "-hiveconf", "hive.exec.max.dynamic.partitions.pernode=100000",
               "-hiveconf", "hive.hadoop.supports.splittable.combineinputformat=true",
               "-hiveconf", "mapreduce.input.fileinputformat.split.maxsize=%d" % ((input_block_size or HiveProcessRunner.DEFAULT_INPUT_BLOCK_SIZE) * 1024 * 1024),
               "-hiveconf", "mapreduce.map.cpu.vcores=2",
               "-hiveconf", "mapreduce.reduce.cpu.vcores=2",
               "-hiveconf", "hive.merge.mapredfiles=%s" % ('true' if consolidate_output else 'false'),
               "-hiveconf", "hive.vectorized.execution.enabled=true",
               "-hiveconf", "hive.vectorized.execution.reduce.enabled=true",
               "-hiveconf", "hive.cbo.enable=true",
               "-hiveconf", "hive.stats.fetch.column.stats=true",
               "-hiveconf", "mapreduce.child.java.opts=-Xmx%(memory)dm -Xms%(memory)dm" % {'memory': task_memory},
               "-hiveconf", "mapreduce.map.java.opts=-Xmx%(memory)dm -Xms%(memory)dm" % {'memory': task_memory},
               "-hiveconf", "mapreduce.reduce.java.opts=-Xmx%(memory)dm -Xms%(memory)dm" % {'memory': task_memory},
               "-hiveconf", "mapreduce.reduce.memory.mb=%d" % int(task_memory * (1 + HiveProcessRunner.TASK_MEMORY_OVERHEAD)),
               "-hiveconf", "mapreduce.map.memory.mb=%d" % int(task_memory * (1 + HiveProcessRunner.TASK_MEMORY_OVERHEAD)),
               "-hiveconf", "mapreduce.task.io.sort.mb=%d" % max(task_memory / 10, 256)
               ]

        if codec:
            cmd += ["-hiveconf", "mapreduce.output.fileoutputformat.compress.codec=" + codec]
        if slow_start_ratio:
            cmd += ["-hiveconf", "mapreduce.job.reduce.slowstart.completedmaps=" + slow_start_ratio]

        common.logger.info('CMD:\n%s' % ' '.join(cmd))
        return self._run_hive(cmd, log_path=log_dir + "/hive.log")
