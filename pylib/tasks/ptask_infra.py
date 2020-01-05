import calendar
import logging
import os
from glob import glob
import re
import shutil
import smtplib
import urllib
import uuid
from copy import copy
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import datetime
import six
import sys
import time
import numpy as np
from dateutil.relativedelta import relativedelta

# Adjust log level
from pylib.common.date_utils import get_dates_range
from pylib.tasks.data import DataArtifact


logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

from invoke import Result
from invoke.exceptions import Failure
from redis import StrictRedis

from pylib.hive.hive_runner import HiveProcessRunner, HiveParamBuilder
from pylib.common.string_utils import random_str
from pylib.hadoop.hdfs_util import test_size, check_success, mark_success, delete_dir, get_file, file_exists, \
    create_client, directory_exists, copy_dir_from_path, calc_desired_partitions, get_size

from pylib.hbase.hbase_utils import validate_records_per_region
from pylib.aws.data_checks import is_s3_folder_big_enough, validate_success, get_s3_folder_size
from pylib.aws.s3 import s3_connection
from pylib.config.SnowflakeConfig import SnowflakeConfig

logger = logging.getLogger('ptask')
logger.addHandler(logging.StreamHandler())


JAVA_PROFILER = '-agentpath:/opt/yjp/bin/libyjpagent.so'


class TasksInfra(object):
    @staticmethod
    def parse_date(date_str, fmt='%Y-%m-%d'):
        return datetime.datetime.strptime(date_str, fmt).date()

    @staticmethod
    def full_partition_path(mode, mode_type, date):
        if mode == 'daily':
            return 'year=%s/month=%s/day=%s' % (str(date.year)[2:], str(date.month).zfill(2), str(date.day).zfill(2))
        elif mode == 'window' or mode_type == 'weekly':
            return 'type=%s/year=%s/month=%s/day=%s' % (
                mode_type, str(date.year)[2:], str(date.month).zfill(2), str(date.day).zfill(2))
        else:
            return 'type=%s/year=%s/month=%s' % (mode_type, str(date.year)[2:], str(date.month).zfill(2))

    @staticmethod
    def year_month_day(date, zero_padding=True):
        if date is None:
            raise AttributeError("date wasn't passed")
        year_str = str(date.year)[2:]
        if zero_padding:
            return 'year=%s/month=%s/day=%s' % (year_str, str(date.month).zfill(2), str(date.day).zfill(2))
        else:
            return 'year=%s/month=%s/day=%s' % (year_str, date.month, date.day)

    @staticmethod
    def year_month_day_country(date, country, zero_padding=True):
        if zero_padding:
            return '%s/country=%s' % (TasksInfra.year_month_day(date, zero_padding=zero_padding), country)

    @staticmethod
    def country_year_month_day(date, country, zero_padding=True):
        return 'country=%s/%s' % (country, TasksInfra.year_month_day(date, zero_padding=zero_padding))

    @staticmethod
    def year_month(date, zero_padding=True):
        if date is None:
            raise AttributeError("date wasn't passed")
        year_str = str(date.year)[2:]
        if zero_padding:
            return 'year=%s/month=%s' % (year_str, str(date.month).zfill(2))
        else:
            return 'year=%s/month=%s' % (year_str, date.month)

    @staticmethod
    def year_months_before(date, months_before, zero_padding=True):
        if date is None:
            raise AttributeError("date wasn't passed")
        actual_month = date - relativedelta(months=months_before)
        year_str = str(actual_month.year)[2:]
        if zero_padding:
            return 'year=%s/month=%s' % (year_str, str(actual_month.month).zfill(2))
        else:
            return 'year=%s/month=%s' % (year_str, actual_month.month)


    @staticmethod
    def year_previous_month(date,  zero_padding=True):
        return TasksInfra.year_months_before(date, 1, zero_padding)

    @staticmethod
    def year_month_previous_day(date, zero_padding=True):
        if date is None:
            raise AttributeError("date wasn't passed")
        previous_day = date - datetime.timedelta(days=1)
        year_str = str(previous_day.year)[2:]
        if zero_padding:
            return 'year=%s/month=%s/day=%s' % (
                year_str, str(previous_day.month).zfill(2), str(previous_day.day).zfill(2))
        else:
            return 'year=%s/month=%s/day=%s' % (year_str, previous_day.month, previous_day.day)

    @staticmethod
    def year_month_before_day(date, delta=1,  zero_padding=True):
        if date is None:
            raise AttributeError("date wasn't passed")
        previous_day = date - datetime.timedelta(days=delta)
        year_str = str(previous_day.year)[2:]
        if zero_padding:
            return 'year=%s/month=%s/day=%s' % (
                year_str, str(previous_day.month).zfill(2), str(previous_day.day).zfill(2))
        else:
            return 'year=%s/month=%s/day=%s' % (year_str, previous_day.month, previous_day.day)

    @staticmethod
    def year_month_next_day(date, zero_padding=True):
        if date is None:
            raise AttributeError("date wasn't passed")
        next_day = date + datetime.timedelta(days=1)
        year_str = str(next_day.year)[2:]
        if zero_padding:
            return 'year=%s/month=%s/day=%s' % (year_str, str(next_day.month).zfill(2), str(next_day.day).zfill(2))
        else:
            return 'year=%s/month=%s/day=%s' % (year_str, next_day.month, next_day.day)

    @staticmethod
    def year_month_country(date, country, zero_padding=True):
        return '%s/country=%s' % (TasksInfra.year_month(date, zero_padding=zero_padding), country)

    @staticmethod
    def country_year_month(date, country, zero_padding=True):
        return 'country=%s/%s' % (country, TasksInfra.year_month(date, zero_padding=zero_padding))

    @staticmethod
    def days_in_range(end_date, mode_type):
        if mode_type == 'daily':
            yield end_date
            return
        elif mode_type.startswith('last-'):
            last_days = int(mode_type[len('last-'):])
            start_date = end_date - datetime.timedelta(days=last_days-1)
        elif mode_type == 'monthly':
            # get last day in month
            last = calendar.monthrange(end_date.year, end_date.month)[1]
            end_date = datetime.datetime(end_date.year, end_date.month, last).date()
            start_date = datetime.datetime(end_date.year, end_date.month, 1).date()
        else:
            raise ValueError("Unable to figure out range from mode_type='%s'" % mode_type)

        for i in range((end_date - start_date).days + 1):
            yield start_date + datetime.timedelta(days=i)

    @staticmethod
    def dates_range_paths(directory, mode, end_date, lookback=None, zero_padding=True):
        if mode == 'snapshot':
            dates_range = get_dates_range(end_date, lookback or 24, step_type='months')
            return [(directory + TasksInfra.year_month(date, zero_padding=zero_padding), date) for date in dates_range]
        elif mode == 'window':
            dates_range = get_dates_range(end_date, lookback or 28)
            return [(directory + TasksInfra.year_month_day(date, zero_padding=zero_padding), date) for date in dates_range]
        else:
            dates_range = get_dates_range(end_date, lookback or 150)
            return [(directory + TasksInfra.year_month_day(date, zero_padding=zero_padding), date) for date in dates_range]

    EXEC_WRAPPERS = {
        'python': '"',
        'java': '\\"\'\\"',
        'bash': "'"
    }

    @staticmethod
    def add_command_params(command, command_params, value_wrap='', *positional):
        ans = command + ' ' + ' '.join(positional)

        for key, value in command_params.items():
            if value is None:
                continue
            if isinstance(value, bool):
                if value:
                    ans += " -%s" % key
            elif isinstance(value, list):
                for elem in value:
                    ans += " -%s %s%s%s" % (key, value_wrap, str(elem), value_wrap)
            else:
                ans += " -%s %s%s%s" % (key, value_wrap, str(value), value_wrap)
        return ans

    @staticmethod
    def add_jvm_options(command, jvm_options):
        if jvm_options:
            for key, value in jvm_options.items():
                command += ' -D {}={}'.format(str(key), str(value))
        return command

    @staticmethod
    def kv(purpose='bigdata', snowflake_env=None):
        from pylib.sw_config.bigdata_kv import get_kv
        return get_kv(purpose, snowflake_env)

    SMTP_SERVER = 'email-smtp.us-east-1.amazonaws.com'
    SMTP_PORT = 587
    SMTP_USER = 'AKIAJTAT2USDRQ5Y5QHA'
    SMTP_PASS = 'AubJwLhz8uhPfBF4/Kz7KI9HezfMMvi7hWuqurUZV5lr'


    @staticmethod
    def send_mail(mail_from, mail_to, mail_subject, content, format='plain', image_attachment=None):
        """
        Send an email with an optional image attachment.

        :param str mail_from: From field for email.
        :param str mail_to: To field for email.
        :param str mail_subject: Subject field for email.
        :param str content: Email's content.
        :param str format: Format for email content. Defaults to plain. Is optional.
        :param str image_attachment: Image as byte string. Is optional.
        """

        if isinstance(mail_to, list):
            mail_to = ','.join(mail_to)

        msg = MIMEMultipart()
        msg.attach(MIMEText(content, format))

        msg['From'] = mail_from
        msg['To'] = mail_to
        msg['Subject'] = mail_subject

        if image_attachment:
            img = MIMEImage(image_attachment)
            msg.attach(img)

        server = smtplib.SMTP(host=TasksInfra.SMTP_SERVER, port=TasksInfra.SMTP_PORT)
        server.starttls()
        server.login(TasksInfra.SMTP_USER, TasksInfra.SMTP_PASS)
        server.sendmail(mail_from, mail_to, msg.as_string())
        server.quit()

    @staticmethod
    def _fix_corrupt_files(corrupt_files, quarantine_dir, remove_last_line=False):
        compression_suffixes = ['.bz2', '.gz', '.deflate', '.snappy']

        def consumer_re():
            consumer_type = 'kafka-consumer'
            return re.compile('.*/app=%s-([a-z]+)([0-9]+)([a-z]+)/*' % consumer_type)

        def adjust_path(path, original):
            try_match = consumer_re().search(original)
            if try_match is None:
                return path
            else:
                node, num, sub_consumer = try_match.groups()
                return path.replace(node + num, node + num + sub_consumer)

        import subprocess
        subprocess.call(['hadoop', 'fs', '-mkdir', '-p', quarantine_dir])
        for corrupt_file in corrupt_files:
            hdfs_dir = '/'.join(corrupt_file.split('/')[:-1])
            relative_name = corrupt_file.split('/')[-1]
            local_file = '/tmp/%s' % relative_name

            for cmp_suff in compression_suffixes:
                if local_file.endswith(cmp_suff):
                    local_file = local_file[:-len(cmp_suff)]
                    break

            with open(local_file, 'w') as temp_writer:
                subprocess.call(['hadoop', 'fs', '-text', corrupt_file], stdout=temp_writer)

            if remove_last_line:
                nixed_local_file = local_file + "_rem"
                with open(nixed_local_file, 'w') as temp_writer:
                    subprocess.call(['head', '-n-1', local_file], stdout=temp_writer)
                local_file = nixed_local_file

            quarantine_path = '%s/%s' % (quarantine_dir, relative_name)
            quarantine_path = adjust_path(quarantine_path, corrupt_file)
            if subprocess.call(['hadoop', 'fs', '-mv', corrupt_file, quarantine_path]) == 0:
                subprocess.call(['hadoop', 'fs', '-put', local_file, hdfs_dir])

    @staticmethod
    def get_last_yarn_application(task_id):
        from pylib.hadoop.yarn_utils import get_applications
        apps = get_applications(applicationTags=task_id)

        def cmp_ts(app1, app2):
            ts1, ts2 = app1['finishedTime'], app2['finishedTime']
            return -1 if ts1 > ts2 else 0 if ts1 == ts2 else 1

        last_app = sorted(apps, cmp=cmp_ts)[0]
        return last_app


    @staticmethod
    def handle_bad_input(mail_recipients=None, report_name=None, remove_last_line=False, app_id=None):
        """
        Mitigates bad input in the operation performed within this context.
        Currently only works if a MapReduce job(s) was run. Salvages the portion of the input which is fine
        The original corrupt files are stored aside and an optional report is sent

        :param mail_recipients: Optional (string or collection of strings).
        if passed, will generate a report sent to the specified recipients
        :param report_name: Prefix on the report to tell which input is corrupt. defaults to the task name
        :return: None
        """

        from pylib.hadoop.yarn_utils import get_applications, get_app_jobs
        from pylib.hadoop.bad_splits import get_corrupt_input_files

        files_to_treat = set()

        # remove following code, move method to ContexualizedTaskInfra, make method non static and use self.task_id
        # once we have no bash clients for it
        import os
        task_id = os.environ['TASK_ID']
        app_to_check = app_id or TasksInfra.get_last_yarn_application(task_id)

        for job in get_app_jobs(app_to_check):
            files_to_treat.update(get_corrupt_input_files(job['job_id']))

        if len(files_to_treat) == 0:
            logging.info('No corrupt files detected')
            return
        else:
            logging.info('Detected corrupt files: %s' % ' '.join(files_to_treat))
            quarantine_dir = '/similargroup/corrupt-data/%s' % task_id
            TasksInfra._fix_corrupt_files(files_to_treat, quarantine_dir, remove_last_line)

            # Report, if asked
            if mail_recipients is not None:
                mail_from = 'dr.file@similarweb.com'
                mail_to = [mail_recipients] if isinstance(mail_recipients, basestring) else mail_recipients
                subject = 'Corrupt Files Report %s' % (report_name or task_id)
                message = '''
Corrupt Files Detected:
%(file_listing)s
All have been repaired. Original Corrupt Files are present on HDFS at %(eviction)s
                    ''' % {
                    'file_listing': '\n'.join(files_to_treat),
                    'eviction': quarantine_dir
                }

                TasksInfra.send_mail(mail_from, mail_to, subject, message)

    @staticmethod
    def repair_single_job_corrupt_input(job_id, quarantine_name=None):
        from pylib.hadoop.bad_splits import get_corrupt_input_files
        quarantine_dir = '/similargroup/corrupt-data/%s' % quarantine_name or random_str(10)
        files_to_treat = get_corrupt_input_files(job_id)

        if len(files_to_treat) == 0:
            logging.info('No corrupt files detected')
            return
        else:
            logging.info('Detected corrupt files: %s' % ' '.join(files_to_treat))
            TasksInfra._fix_corrupt_files(files_to_treat, quarantine_dir)

    @staticmethod
    def get_rserve_host():
        return TasksInfra.kv().get('services/rserve/host')

    @staticmethod
    def get_rserve_port():
        return TasksInfra.kv().get('services/rserve/port')

    @staticmethod
    def get_mr_partitions_config_key():
        return 'mapreduce.job.reduces'

    @staticmethod
    def get_spark_partitions_config_key():
        return 'spark.sw.appMasterEnv.numPartitions'


class ContextualizedTasksInfra(object):
    def __init__(self, ctx):
        """
        :param ctx: invoke.context.Context
        """
        self.ctx = ctx
        self.redis = None
        self.jvm_opts = {}
        self.hadoop_configs = {}

    def __compose_infra_command(self, command):
        ans = 'source %s/scripts/common.sh && %s' % (self.execution_dir, command)
        return ans

    def __with_rerun_root_queue(self, command):
        return 'source %s/scripts/common.sh && setRootQueue reruns && %s' % (self.execution_dir, command)

    def __compose_hadoop_runner_command(self, jar_path, jar_name, main_class, command_params, override_jvm_opts=None,
                                        rerun_root_queue=False):
        command = self.__compose_infra_command(
            'execute hadoopexec %(base_dir)s/%(jar_relative_path)s %(jar)s %(class)s' %
            {
                'base_dir': self.execution_dir,
                'jar_relative_path': jar_path,
                'jar': jar_name,
                'class': main_class
            }
        )

        if override_jvm_opts is None:
            override_jvm_opts = {}

        if self.should_profile:
            override_jvm_opts['mapreduce.reduce.java.opts'] = JAVA_PROFILER
            override_jvm_opts['mapreduce.map.java.opts'] = JAVA_PROFILER

        curr_jvm_opts = copy(self.jvm_opts)
        curr_jvm_opts.update(override_jvm_opts)
        curr_jvm_opts.update(self.hadoop_configs)
        command = TasksInfra.add_jvm_options(command, curr_jvm_opts)
        command = TasksInfra.add_command_params(command, command_params, value_wrap=TasksInfra.EXEC_WRAPPERS['java'])
        if rerun_root_queue:
            command = self.__with_rerun_root_queue(command)
        return command

    def __is_hdfs_collection_valid(self, directories, min_size_bytes=0, validate_marker=False, is_strict=False):
        ans = True
        if isinstance(directories, list):
            for directory in directories:
                ans = ans and self.__is_hdfs_collection_valid(directory, min_size_bytes, validate_marker)
            return ans

        # leaf mode
        directory = directories
        if self.dry_run:
            log_message = "Dry Run: would have checked that '%s' size > %d bytes" % (directory, min_size_bytes)
            log_message += ' and contains _SUCCESS file' if validate_marker else ''
            log_message += '\n'
            sys.stdout.write(log_message)
        else:
            if validate_marker:
                ans = ans and check_success(directory)
            if min_size_bytes > 0:
                ans = ans and test_size(directory, min_size_bytes, is_strict)
        return ans

    def is_valid_output_exists(self, directories, min_size_bytes=0, validate_marker=False):
        self.log_lineage_hdfs(directories, 'output')
        return self.__is_hdfs_collection_valid(directories, min_size_bytes, validate_marker)

    def clear_output_dirs(self, output_dirs):
        if output_dirs is not None:
            assert isinstance(output_dirs, list), "Output dirs need to be passed in a list."
            for dir in output_dirs:
                if not (self.dry_run or self.checks_only):
                    delete_dir(dir)
                else:
                    sys.stdout.write("Dry Run: would deleted output folder: %s" % dir)

    def is_valid_input_exists(self, directories, min_size_bytes=0, validate_marker=False):
        self.log_lineage_hdfs(directories, 'input')
        return self.__is_hdfs_collection_valid(directories, min_size_bytes, validate_marker)

    def __compose_python_runner_command(self, python_executable, command_params, *positional):
        command = self.__compose_infra_command('pyexecute %s/%s' % (self.execution_dir, python_executable))
        command = TasksInfra.add_command_params(command, command_params, TasksInfra.EXEC_WRAPPERS['python'],
                                                *positional)
        return command

    def __get_common_args(self):
        return self.ctx.config.config['sw_common']

    def log_lineage_hdfs(self, directories, direction):
        if self.dry_run or self.checks_only:
            sys.stdout.write('(*)')
            return
        if self.has_task_id is False:
            return
        if self.execution_user != 'airflow':
            return
        lineage_value_template = \
            '%(execution_user)s.%(dag_id)s.%(task_id)s.%(execution_dt)s::%(direction)s:hdfs::%(directory)s'

        lineage_key = 'LINEAGE_%s' % datetime.date.today().strftime('%y-%m-%d')

        # Barak: this is not good we don't want to ignore lineage reporting
        try:
            if isinstance(directories, list):
                for directory in directories:
                    lineage_value = lineage_value_template % {
                        'execution_user': self.execution_user,
                        'dag_id': self.dag_id,
                        'task_id': self.task_id,
                        'execution_dt': self.execution_dt,
                        'directory': directory,
                        'direction': direction
                    }
                    self.get_redis_client().rpush(lineage_key, lineage_value)
        except:
            logger.error('failed reporting lineage')

    def get_redis_client(self):
        if self.redis is None:
            self.redis = StrictRedis(host=SnowflakeConfig().get_service_name(service_name='redis-bigdata'),
                                     socket_timeout=15,
                                     socket_connect_timeout=15,
                                     retry_on_timeout=True)
        return self.redis

    def is_valid_redis_output(self, prefix, count):
        return self.__get_prefix_keys_count(prefix, count) >= count

    def assert_redis_keys_validity(self, prefix, count):
        cnt = self.__get_prefix_keys_count(prefix, count)
        assert cnt == count, 'Only %d keys with prefix %s' % (cnt, prefix)

    def __get_prefix_keys_count(self, prefix, count):
        print('Checking if there are at least %d keys with %s prefix in Redis...' % (count, prefix))
        cnt = 0
        for _ in self.get_redis_client().scan_iter(match='%s*' % prefix, count=count):
            cnt += 1
            if cnt == count:
                break
        return cnt

    def report_lineage(self, direction, paths_sizes):
        self.log_lineage_hdfs(paths_sizes.keys(), direction)

    def assert_input_validity(self, directories, min_size_bytes=0, validate_marker=False, is_strict=False):
        if isinstance(directories, six.string_types):
            directories = [directories]

        self.report_lineage('input', {directory: None for directory in directories})
        assert self.__is_hdfs_collection_valid(directories,
                                               min_size_bytes=min_size_bytes,
                                               validate_marker=validate_marker,
                                               is_strict=is_strict) is True, \
            'Input is not valid, given value is %s' % directories

    def assert_output_validity(self, directories, min_size_bytes=0, validate_marker=False, is_strict=False):
        if isinstance(directories, six.string_types):
            directories = [directories]

        self.report_lineage('output', {directory: None for directory in directories})
        assert self.__is_hdfs_collection_valid(directories,
                                               min_size_bytes=min_size_bytes,
                                               validate_marker=validate_marker,
                                               is_strict=is_strict) is True, \
            'Output is not valid, given value is %s' % directories

    # ----------- HBASE -----------
    def hbase_table_full_name(self, name):
        return self.table_prefix + name + self.table_suffix

    def hbase_table_normalized_name(self, name, add_branch_name_prefix=True):
        # Legacy convention should be preserved for now in the production branch
        if add_branch_name_prefix and self.branch != 'dzhdam2':
            return self.branch + "_" + name + self.table_suffix
        else:
            return name + self.table_suffix

    def assert_hbase_table_valid(self, table_name, columns=None, minimum_regions_count=30, rows_per_region=50,
                                 cluster_name=None):
        if self.dry_run:
            log_message = "Dry Run: would have checked that table '%s' has %d regions and %d keys per region" % (
                table_name, minimum_regions_count, rows_per_region)
            log_message += ' in columns: %s' % ','.join(columns) if columns else ''
            log_message += '\n'
            sys.stdout.write(log_message)
        else:
            assert validate_records_per_region(table_name, columns, minimum_regions_count, rows_per_region,
                                               cluster_name), \
                'hbase table content is not valid, table name: %s' % table_name

    def assert_hbase_snapshot_exists(self, snapshot_name, hbase_root='/hbase', name_node=None):
        snapshot_params = {'snapshot_name': snapshot_name, 'hbase_root': hbase_root}
        snapshot_params['snapshot_path'] = snapshot_path = '{hbase_root}/.hbase-snapshot/{snapshot_name}/'.format(
            **snapshot_params)
        hdfs_client = create_client(name_node=name_node) if name_node else None

        if self.dry_run:
            print(
                "Dry run: would have checked that {snapshot_name} exists at {snapshot_path}".format(**snapshot_params))
        else:
            print("validating existence of snapshotinfo and manifest in {snapshot_path}".format(**snapshot_params))
            assert file_exists(file_path=snapshot_path + '.snapshotinfo', hdfs_client=hdfs_client) and \
                   file_exists(file_path=snapshot_path + 'data.manifest', hdfs_client=hdfs_client), \
                'hbase snapshot not found in path {snapshot_path}'.format(**snapshot_params)

            print('snapshot exists')

    def run_distcp(self, source, target, mappers=20, overwrite=True):
        job_name_property = " -D'mapreduce.job.name=distcp {source} {target}'".format(source=source, target=target)
        curr_jvm_opts = copy(self.jvm_opts)
        curr_jvm_opts.update(self.hadoop_configs)
        jvm_opts = TasksInfra.add_jvm_options(job_name_property, curr_jvm_opts)
        distcp_opts = "-m {mappers} ".format(mappers=mappers)
        if overwrite:
            if self.dry_run:
                print("Dry run: would have deleted " + target)
            else:
                delete_dir(path=target)

        cmd = 'hadoop distcp {jvm_opts} {distcp_opts} {source_path} {target_path}'.format(
            jvm_opts=jvm_opts,
            distcp_opts=distcp_opts,
            source_path=source,
            target_path=target
        )
        self.run_bash(cmd)

    def run_hadoop(self, jar_path, jar_name, main_class, command_params, determine_reduces_by_output=False, jvm_opts=None, default_num_reducers=200):
        command_params, jvm_opts = self.determine_mr_output_partitions(command_params, determine_reduces_by_output, jvm_opts, default_num_reducers)
        return self.run_bash(
            self.__compose_hadoop_runner_command(
                jar_path=jar_path,
                jar_name=jar_name,
                main_class=main_class,
                command_params=command_params,
                override_jvm_opts=jvm_opts,
                rerun_root_queue=self.rerun)
        ).ok

    # managed_output_dirs - dirs to be deleted on start and then marked upon a successful conclusion
    def run_hive(self, query, hive_params=HiveParamBuilder(), query_name='query', partitions=32, query_name_suffix=None,
                 managed_output_dirs=None, cache_files=None, aux_jars=None, **extra_hive_conf):
        if managed_output_dirs is None:
            managed_output_dirs = []
        elif isinstance(managed_output_dirs, basestring):
            managed_output_dirs = [managed_output_dirs]

        extra_hive_conf.update(self.jvm_opts)
        extra_hive_conf.update(self.hadoop_configs)

        if self.rerun:
            hive_params = hive_params.as_rerun()
        if self.should_profile:
            hive_params = hive_params.add_child_option('-agentpath:/opt/yjp/bin/libyjpagent.so')

        job_name = 'Hive. %s - %s - %s' % (query_name, self.date_title, self.mode)
        if query_name_suffix is not None:
            job_name = job_name + ' ' + query_name_suffix

        # delete output on start (supports dr and co)
        self.clear_output_dirs(managed_output_dirs)

        log_dir = '/tmp/logs/%s' % random_str(5)
        os.mkdir(log_dir)

        cache_dir = '/tmp/cache/%s' % random_str(5)
        os.makedirs(cache_dir)

        if cache_files is not None:
            # register cached files
            for cached_file in cache_files:
                if '#' in cached_file:  # handle renaming
                    hdfs_path, target_name = cached_file.split('#')
                else:
                    hdfs_path = cached_file
                    target_name = cached_file.split('/')[-1]

                get_file(hdfs_path, '%s/%s' % (cache_dir, target_name))
                sys.stdout.write('caching hdfs file %s as %s' % (cached_file, target_name))
                query = 'ADD FILE %s/%s; \n%s' % (cache_dir, target_name, query)

        HiveProcessRunner().run_query(query, hive_params, job_name=job_name, partitions=partitions, log_dir=log_dir,
                                      is_dry_run=self.dry_run or self.checks_only, aux_jars=aux_jars, **extra_hive_conf)
        for mdir in managed_output_dirs:
            self.mark_success(mdir)

        shutil.rmtree(log_dir)
        shutil.rmtree(cache_dir)

    @staticmethod
    def fail(reason=None):
        if reason is not None:
            assert False, reason
        else:
            assert False

    # Todo: Move it to the mobile project
    def run_mobile_hadoop(self, command_params,
                          main_class='com.similargroup.mobile.main.MobileRunner',
                          jvm_opts=None,
                          determine_reduces_by_output=False,
                          default_num_reducers=200,
                          rerun_root_queue=False):
        return self.run_hadoop(jar_path='mobile',
                               jar_name='mobile.jar',
                               main_class=main_class,
                               command_params=command_params,
                               determine_reduces_by_output=determine_reduces_by_output,
                               default_num_reducers=default_num_reducers,
                               jvm_opts=jvm_opts)

    def run_analytics_hadoop(self, command_params, main_class, determine_reduces_by_output=False, default_num_reducers=200, jvm_opts=None):
        return self.run_hadoop(
            jar_path='analytics',
            jar_name='analytics.jar',
            main_class=main_class,
            command_params=command_params,
            determine_reduces_by_output=determine_reduces_by_output,
            default_num_reducers=default_num_reducers,
            jvm_opts=jvm_opts)

    def run_bash(self, command):
        sys.stdout.write("#####\nFinal bash command: \n-----------------\n%s\n#####\n" % command)
        sys.stdout.flush()
        time.sleep(1)
        if self.dry_run or self.checks_only:
            return Result(command, stdout=None, stderr=None, exited=0, pty=None)
        return self.ctx.run(command)

    def run_python(self, python_executable, command_params, *positional):
        return self.run_bash(self.__compose_python_runner_command(python_executable, command_params, *positional)).ok

    def run_r(self, r_executable, command_params):
        return self.run_bash(self.__compose_infra_command(
            "execute Rscript %s/%s %s" % (self.execution_dir, r_executable, ' '.join(command_params)))).ok

    def run_r_on_rserve(self, r_executable, command_params=None):
        current_file_path = os.path.abspath(os.path.dirname(__file__))
        run_rserve = '%s/resources/RunRserve.R' % current_file_path
        rserve_host = TasksInfra.get_rserve_host()
        rserve_port = TasksInfra.get_rserve_port()
        executable_path = '%s/%s' % (self.execution_dir, r_executable) if self.execution_dir else r_executable
        return self.run_bash(self.__compose_infra_command(
            'execute Rscript %s %s %s %s %s' % (run_rserve, rserve_host, rserve_port,
                                                executable_path,
                                                ' '.join(command_params) if command_params else ""))).ok

    def dates_range_paths(self, directory, lookback=None):
        return TasksInfra.dates_range_paths(directory, self.mode, self.date, lookback)

    def latest_success_size_for_path(self, directory, mode=None, start_date=None, lookback=None, min_size_bytes=None, sub_dir=""):

        self.set_s3_keys()

        mode = self.mode if mode is None else mode
        start_date = self.date if start_date is None else start_date
        print("mode: " + mode)
        for path, date in reversed(TasksInfra.dates_range_paths(directory, mode, start_date, lookback)):
                final_path = path + sub_dir
                print("Try to find latest success in: " + final_path)
                path_data_artifact = DataArtifact(final_path, required_size=min_size_bytes or 1)
                check_size = path_data_artifact.check_size()
                if check_size:
                    print("latest success date for %s is %s" % (directory, date))
                    return path_data_artifact.resolved_path, path_data_artifact.actual_size, date
        print("No latest success date found for %s" % directory)
        return None, None, None


    def latest_daily_success_date(self, directory, month_lookback, date=None):
        """
        Get the latest success date of a task by searching the HDFS for _success markers
        :param directory: The HDFS base dir to look at
        :param month_lookback: lower bound month to look at
        :param date: upper bound day. The default behavior uses this.date.
        :return: a datetime.date if a valid date is found, else None
        """
        d = date.strftime('%Y-%m-%d') if date is not None else self.date
        command = self.__compose_infra_command('LatestDailySuccessDate %s %s %s' % (directory, d, month_lookback))
        try:
            date_str = self.run_bash(command=command).stdout.strip()
            return TasksInfra.parse_date(date_str) if date_str else None
        except Failure:
            return None

    def latest_monthly_success_date(self, directory, month_lookback, date=None):
        """ Similar to latest_daily_success_date, but returns the 1st of the month"""
        d = date.strftime('%Y-%m-%d') if date is not None else self.date
        command = self.__compose_infra_command('LatestMonthlySuccessDate %s %s %s' % (directory, d, month_lookback))
        try:
            date_str = self.run_bash(command=command).stdout.strip()
            return TasksInfra.parse_date(date_str).replace(day=1) if date_str else None
        except Failure:
            return None

    @staticmethod
    def __latest_success_date_kv(base_path, fmt, days_lookback=None, date=None):
        marked_dates_str = sorted(TasksInfra.kv().sub_keys(base_path), reverse=True)
        for marked_date_str in marked_dates_str:
            marked_date = TasksInfra.parse_date(marked_date_str, fmt)
            if (not date) or (marked_date <= date):
                if (not days_lookback) or (marked_date + datetime.timedelta(days=days_lookback) >= date):
                    if TasksInfra.kv().get('%s/%s' % (base_path, marked_date_str)) == 'success':
                        return marked_date

        return None

    def latest_daily_success_date_kv(self, base_path, days_lookback=90, date=None):
        """
        Get the latest success date of a task by searching the KV store for success markers
        :param base_path: the path in the KV store to look at
        :param days_lookback: lower bound days to look at
        :param date: upper bound day. The default behavior uses this.date.
        :return: a datetime.date if a valid date is found, else None
        """
        if not date:
            date = self.date
        return ContextualizedTasksInfra.__latest_success_date_kv(base_path,
                                                                 fmt='%Y-%m-%d',
                                                                 days_lookback=days_lookback,
                                                                 date=date)

    def latest_monthly_success_date_kv(self, base_path, days_lookback=90, date=None):
        """" Similar to latest_daily_success_date, but returns the 1st of the month"""
        if not date:
            date = self.date
        return ContextualizedTasksInfra.__latest_success_date_kv(base_path,
                                                                 fmt='%Y-%m',
                                                                 days_lookback=days_lookback,
                                                                 date=date)

    def mark_success(self, directory, opts=''):
        if self.dry_run or self.checks_only:
            sys.stdout.write('''Dry Run: If successful would create '%s/_SUCCESS' marker\n''' % directory)
        else:
            mark_success(directory)


    # --- path partitions ----
    def full_partition_path(self):
        return TasksInfra.full_partition_path(self.__get_common_args()['mode'], self.__get_common_args()['mode_type'],
                                              self.__get_common_args()['date'])

    def get_date_suffix(self, date=None, mode=None, zero_padding=True):
        m = mode or self.mode
        dt = date or self.date
        if m == 'snapshot':
            return TasksInfra.year_month(dt, zero_padding)
        else:
            return TasksInfra.year_month_day(dt, zero_padding)

    def year_month_day(self, date=None, zero_padding=True):
        return TasksInfra.year_month_day(self.__get_common_args()['date'] if date is None else date,
                                         zero_padding=zero_padding)

    ymd = year_month_day

    def year_month_day_country(self, country, zero_padding=True):
        return TasksInfra.year_month_day_country(self.__get_common_args()['date'], country,
                                                 zero_padding=zero_padding)

    def year_month_country(self, country, zero_padding=True):
        return TasksInfra.year_month_country(self.__get_common_args()['date'], country,
                                             zero_padding=zero_padding)

    def country_year_month_day(self, country, date=False, zero_padding=True):
        if date==False:
            date = self.__get_common_args()['date']
        return TasksInfra.country_year_month_day(date, country,
                                                 zero_padding=zero_padding)

    def country_year_month(self, country, date=False, zero_padding=True):
        if date==False:
            date = self.__get_common_args()['date']
        return TasksInfra.country_year_month(date, country,
                                             zero_padding=zero_padding)

    def year_month(self, zero_padding=True, date=None):
        return TasksInfra.year_month(self.__get_common_args()['date'] if date is None else date,
                                     zero_padding=zero_padding)

    def year_previous_month(self, zero_padding=True):
        return TasksInfra.year_previous_month(self.__get_common_args()['date'],
                                              zero_padding=zero_padding)

    def year_months_before(self, months_before, zero_padding=True):
        return TasksInfra.year_months_before(self.__get_common_args()['date'],
                                             months_before=months_before, zero_padding=zero_padding)

    def year_month_previous_day(self, zero_padding=True):
        return TasksInfra.year_month_previous_day(self.__get_common_args()['date'],
                                                  zero_padding=zero_padding)

    def year_month_before_day(self, delta=1, zero_padding=True):
        return TasksInfra.year_month_before_day(self.__get_common_args()['date'], delta=delta,
        zero_padding = zero_padding)

    def year_month_next_day(self, zero_padding=True):
        return TasksInfra.year_month_next_day(self.__get_common_args()['date'],
                                              zero_padding=zero_padding)

    def date_suffix_by_mode(self, date=None):
        return self.year_month(date=date) if self.mode == 'snapshot' else self.year_month_day(date=date)

    ym = year_month

    # --- dates ----
    def days_in_range(self):
        end_date = self.__get_common_args()['date']
        mode_type = self.__get_common_args()['mode_type']

        return TasksInfra.days_in_range(end_date, mode_type)

    def get_sw_repos(self):
        # Similarweb default repositories
        return ["https://nexus.similarweb.io/repository/similar-bigdata/"]

    # module is either 'mobile' or 'analytics'
    def run_spark2(self,
                   main_class,
                   module,
                   queue,
                   app_name,
                   command_params,
                   jars_from_lib=None,
                   files=None,
                   spark_configs=None,
                   named_spark_args=None,
                   determine_partitions_by_output=None,
                   packages=None,
                   managed_output_dirs=None,
                   repositories=None,
                   java_opts=" -Xms16m"):
        jar = './%s.jar' % module
        jar_path = '%s/%s' % (self.execution_dir, module)

        spark_submit_opts = os.getenv("SPARK_SUBMIT_OPTS")
        if not spark_submit_opts:
            spark_submit_opts = ""
        os.environ["SPARK_SUBMIT_OPTS"] = spark_submit_opts + " " + java_opts

        # delete output on start
        self.clear_output_dirs(managed_output_dirs)

        command_params, spark_configs = self.determine_spark_output_partitions(command_params,
                                                                               determine_partitions_by_output,
                                                                               spark_configs)
        additional_configs = self.build_spark_additional_configs(named_spark_args, spark_configs)

        yarn_tags = os.environ['YARN_TAGS'] if 'YARN_TAGS' in os.environ else ''
        snowflake_cur_env = os.environ.get('SNOWFLAKE_ENV')

        command = 'cd %(jar_path)s;spark2-submit' \
                  ' --queue %(queue)s' \
                  ' --conf "spark.yarn.appMasterEnv.SNOWFLAKE_ENV=%(snowflake_env)s"' \
                  ' --conf "spark.executorEnv.SNOWFLAKE_ENV=%(snowflake_env)s"' \
                  ' --conf "spark.yarn.tags=%(yarn_application_tags)s"' \
                  ' --name "%(app_name)s"' \
                  ' --master yarn-cluster' \
                  ' --deploy-mode cluster' \
                  ' %(add_opts)s ' \
                  ' --jars %(jars)s' \
                  ' --files "%(files)s"' \
                  '%(extra_pkg_cmd)s' \
                  '%(extra_repo_cmd)s' \
                  ' --class %(main_class)s' \
                  ' %(jar)s ' % \
                  {
                      'jar_path': jar_path,
                      'queue': queue,
                      'app_name': app_name,
                      'add_opts': additional_configs,
                      'snowflake_env': snowflake_cur_env,
                      'jars': self.get_jars_list(jar_path, jars_from_lib),
                      'files': ','.join(files or []),
                      'extra_pkg_cmd': (' --packages %s' % ','.join(packages)) if packages is not None else '',
                      'extra_repo_cmd': ' --repositories %s' % ','.join((repositories if repositories is not None else []) + self.get_sw_repos()),
                      'main_class': main_class,
                      'jar': jar,
                      'yarn_application_tags': yarn_tags
                  }

        command = TasksInfra.add_command_params(command, command_params,
                                                value_wrap=TasksInfra.EXEC_WRAPPERS['bash'])
        return self.run_bash(command).ok

    # module is either 'mobile' or 'analytics'
    def run_spark(self,
                  main_class,
                  module,
                  queue,
                  app_name,
                  command_params,
                  jars_from_lib=None,
                  files=None,
                  spark_configs=None,
                  named_spark_args=None,
                  determine_partitions_by_output=None,
                  packages=None,
                  managed_output_dirs=None):
        jar = './%s.jar' % module
        jar_path = '%s/%s' % (self.execution_dir, module)

        # delete output on start
        self.clear_output_dirs(managed_output_dirs)

        command_params, spark_configs = self.determine_spark_output_partitions(command_params, determine_partitions_by_output, spark_configs)
        additional_configs = self.build_spark_additional_configs(named_spark_args, spark_configs)

        yarn_tags = os.environ['YARN_TAGS'] if 'YARN_TAGS' in os.environ else ''
        snowflake_cur_env = os.environ.get('SNOWFLAKE_ENV')

        command = 'cd %(jar_path)s;spark-submit' \
                  ' --queue %(queue)s' \
                  ' --conf "spark.yarn.appMasterEnv.SNOWFLAKE_ENV=%(snowflake_env)s"' \
                  ' --conf "spark.executorEnv.SNOWFLAKE_ENV=%(snowflake_env)s"' \
                  ' --conf "spark.yarn.tags=%(yarn_application_tags)s"' \
                  ' --name "%(app_name)s"' \
                  ' --master yarn-cluster' \
                  ' --deploy-mode cluster' \
                  ' %(add_opts)s ' \
                  ' --jars %(jars)s' \
                  ' --files "%(files)s"' \
                  '%(extra_pkg_cmd)s' \
                  ' --class %(main_class)s' \
                  ' %(jar)s ' % \
                  {
                      'jar_path': jar_path,
                      'queue': queue,
                      'app_name': app_name,
                      'add_opts': additional_configs,
                      'snowflake_env': snowflake_cur_env,
                      'jars': self.get_jars_list(jar_path, jars_from_lib),
                      'files': ','.join(files or []),
                      'extra_pkg_cmd': (' --packages %s' % ','.join(packages)) if packages is not None else '',
                      'main_class': main_class,
                      'jar': jar,
                      'yarn_application_tags': yarn_tags
                  }

        command = TasksInfra.add_command_params(command, command_params, value_wrap=TasksInfra.EXEC_WRAPPERS['bash'])
        return self.run_bash(command).ok

    @staticmethod
    def match_jar(jar, jars_in_dir):
        matched_jars = []
        for dir_jar in jars_in_dir:
            if dir_jar.startswith(jar.replace('.jar', '')):
                matched_jars.append(dir_jar)
        return matched_jars

    @staticmethod
    def match_jars_from_lib(jars_from_lib, jars_in_dir):
        matches = []
        unmatched = []
        for jar in jars_from_lib:
            match = ContextualizedTasksInfra.match_jar(jar, jars_in_dir)
            if match:
                matches.extend(match)
            else:
                unmatched.append(jar)
        assert len(unmatched) == 0, "The following jars were not found: %s" % ', '.join(unmatched)
        return matches

    def get_jars_list(self, module_dir, jars_from_lib):
        """
        Returns a list of jars for a given module_dir. If jars_from_lib is not provided, returns a string of
        paths of all jars from the appropriate library folder. If jars_from_lib is specified, accepts a list of
        jars and matches the provided jars with existing jars in the module lib directory.
        :param module_dir: The module directory, ie: analytics or mobile
        :type module_dir: str

        :param jars_from_lib: Library jars you want to find
        :type jars_from_lib: list

        :return: str
        """
        lib_module_dir = '%s/lib' % module_dir

        jars_in_dir = os.listdir(lib_module_dir)

        if jars_from_lib:
            if self.dry_run or self.checks_only:
                selected_jars = ContextualizedTasksInfra.match_jars_from_lib(jars_from_lib, jars_in_dir)
                print('Dry run: Would try and attach the following jars ' + ''.join(selected_jars))
                selected_jars = []
            else:
                selected_jars = ContextualizedTasksInfra.match_jars_from_lib(jars_from_lib, jars_in_dir)
        else:
            if self.dry_run or self.checks_only:
                print('Dry Run: Would attach jars from ' + lib_module_dir)
                selected_jars = []
            else:
                selected_jars = jars_in_dir
        jars = ','.join(map(lambda x: module_dir + '/lib/' + x, selected_jars))
        return jars

    def run_py_spark2(self,
                     main_py_file,
                     app_name=None,
                     command_params=None,
                     files=None,
                     include_main_jar=True,
                     jars_from_lib=None,
                     module='mobile',
                     named_spark_args=None,
                     packages=None,
                     repositories=None,
                     py_files=None,
                     py_modules=None,
                     spark_configs=None,
                     use_bigdata_defaults=False,
                     queue=None,
                     determine_partitions_by_output=False,
                     managed_output_dirs=None,
                     additional_artifacts=None
                     ):

        # delete output on start
        self.clear_output_dirs(managed_output_dirs)

        command_params, spark_configs = self.determine_spark_output_partitions(command_params, determine_partitions_by_output, spark_configs)
        additional_configs = self.build_spark_additional_configs(named_spark_args, spark_configs)

        final_py_files = py_files or []

        module_dir = self.execution_dir + '/' + module
        exec_py_file = 'python/sw_%s/%s' % (module.replace('-', '_'), main_py_file) if use_bigdata_defaults else main_py_file

        py_modules = py_modules or []
        if use_bigdata_defaults:
            py_modules.append(module)

        ##if determine_partitions_by_output:
        ##    py_modules.append('sw-spark-common')

        for requested_module in py_modules:
            req_mod_dir = self.execution_dir + '/' + requested_module
            egg_files = glob('%s/*.egg' % req_mod_dir)
            final_py_files.extend(egg_files)
            #todo change to assert
            if len(egg_files) == 0:
                print('failed finding egg file for requested python module %s. skipping' % requested_module)

        if additional_artifacts is None:
            additional_artifacts = []

        additional_artifacts_paths = []
        for artifact in additional_artifacts:
            artifact_path = '/tmp/%s-%s.egg' % (str(uuid.uuid4()), artifact)
            artifact_url = 'https://pypi-registry.similarweb.io/repository/similar-pypi/packages/%(artifact)s/1.0.0/%(artifact)s-1.0.0-py2.7.egg' % \
                           {'artifact': artifact}
            opener = urllib.URLopener()
            opener.retrieve(artifact_url, artifact_path)
            final_py_files.append(artifact_path)
            additional_artifacts_paths.append(artifact_path)

        if len(final_py_files) == 0:
            py_files_cmd = ' '
        else:
            py_files_cmd = ' --py-files "%s"' % ','.join(final_py_files)

        yarn_tags = os.environ['YARN_TAGS'] if 'YARN_TAGS' in os.environ else ''

        snowflake_cur_env = os.environ.get('SNOWFLAKE_ENV')

        command = 'spark2-submit' \
                  ' --name "%(app_name)s"' \
                  ' --master yarn-cluster' \
                  ' %(queue)s' \
                  ' --conf "spark.yarn.appMasterEnv.SNOWFLAKE_ENV=%(snowflake_env)s"' \
                  ' --conf "spark.executorEnv.SNOWFLAKE_ENV=%(snowflake_env)s"' \
                  ' --conf "spark.yarn.tags=%(yarn_application_tags)s"' \
                  ' --deploy-mode cluster' \
                  ' --jars "%(jars)s"' \
                  ' --files "%(files)s"' \
                  '%(extra_pkg_cmd)s' \
                  '%(extra_repo_cmd)s' \
                  ' %(py_files_cmd)s' \
                  ' %(spark-confs)s' \
                  ' "%(execution_dir)s/%(main_py)s"' \
                  % {
                      'app_name': app_name if app_name else os.path.basename(main_py_file),
                      'execution_dir': module_dir,
                      'queue': '--queue %s' % queue if queue else '',
                      'snowflake_env': snowflake_cur_env,
                      'files': ','.join(files or []),
                      'py_files_cmd': py_files_cmd,
                      'extra_pkg_cmd': (' --packages %s' % ','.join(packages)) if packages is not None else '',
                      'extra_repo_cmd': ' --repositories %s' % ','.join((repositories if repositories is not None else []) + self.get_sw_repos()),
                      'spark-confs': additional_configs,
                      'jars': self.get_jars_list(module_dir, jars_from_lib) + (
                              ',%s/%s.jar' % (module_dir, module)) if include_main_jar else '',
                      'main_py': exec_py_file,
                      'yarn_application_tags': yarn_tags
                  }

        command = TasksInfra.add_command_params(command, command_params, value_wrap=TasksInfra.EXEC_WRAPPERS['python'])
        res = self.run_bash(command).ok
        for artifact_path in additional_artifacts_paths:
            os.remove(artifact_path)
        return res

    def run_py_spark(self,
                     main_py_file,
                     app_name=None,
                     command_params=None,
                     files=None,
                     include_main_jar=True,
                     jars_from_lib=None,
                     module='mobile',
                     named_spark_args=None,
                     packages=None,
                     py_files=None,
                     py_modules=None,
                     spark_configs=None,
                     use_bigdata_defaults=False,
                     queue=None,
                     determine_partitions_by_output=False,
                     managed_output_dirs=None,
                     additional_artifacts=None
                     ):

        # delete output on start
        self.clear_output_dirs(managed_output_dirs)

        command_params, spark_configs = self.determine_spark_output_partitions(command_params, determine_partitions_by_output, spark_configs)
        additional_configs = self.build_spark_additional_configs(named_spark_args, spark_configs)

        final_py_files = py_files or []

        module_dir = self.execution_dir + '/' + module
        exec_py_file = 'python/sw_%s/%s' % (module.replace('-', '_'), main_py_file) if use_bigdata_defaults else main_py_file

        py_modules = py_modules or []
        if use_bigdata_defaults:
            py_modules.append(module)

        if determine_partitions_by_output:
            py_modules.append('sw-spark-common')

        for requested_module in py_modules:
            req_mod_dir = self.execution_dir + '/' + requested_module
            egg_files = glob('%s/*.egg' % req_mod_dir)
            final_py_files.extend(egg_files)
            #todo change to assert
            if len(egg_files) == 0:
                print('failed finding egg file for requested python module %s. skipping' % requested_module)

        if additional_artifacts is None:
            additional_artifacts = []

        additional_artifacts_paths = []
        for artifact in additional_artifacts:
            artifact_path = '/tmp/%s-%s.egg' % (str(uuid.uuid4()), artifact)
            artifact_url = 'https://pypi-registry.similarweb.io/repository/similar-pypi/packages/%(artifact)s/1.0.0/%(artifact)s-1.0.0-py2.7.egg' % \
                           {'artifact': artifact}
            opener = urllib.URLopener()
            opener.retrieve(artifact_url, artifact_path)
            final_py_files.append(artifact_path)
            additional_artifacts_paths.append(artifact_path)

        if len(final_py_files) == 0:
            py_files_cmd = ' '
        else:
            py_files_cmd = ' --py-files "%s"' % ','.join(final_py_files)

        yarn_tags = os.environ['YARN_TAGS'] if 'YARN_TAGS' in os.environ else ''

        snowflake_cur_env = os.environ.get('SNOWFLAKE_ENV')

        command = 'spark-submit' \
                  ' --name "%(app_name)s"' \
                  ' --master yarn-cluster' \
                  ' %(queue)s' \
                  ' --conf "spark.yarn.appMasterEnv.SNOWFLAKE_ENV=%(snowflake_env)s"' \
                  ' --conf "spark.executorEnv.SNOWFLAKE_ENV=%(snowflake_env)s"' \
                  ' --conf "spark.yarn.tags=%(yarn_application_tags)s"' \
                  ' --deploy-mode cluster' \
                  ' --jars "%(jars)s"' \
                  ' --files "%(files)s"' \
                  '%(extra_pkg_cmd)s' \
                  ' %(py_files_cmd)s' \
                  ' %(spark-confs)s' \
                  ' "%(execution_dir)s/%(main_py)s"' \
                  % {
                      'app_name': app_name if app_name else os.path.basename(main_py_file),
                      'execution_dir': module_dir,
                      'queue': '--queue %s' % queue if queue else '',
                      'snowflake_env': snowflake_cur_env,
                      'files': ','.join(files or []),
                      'py_files_cmd': py_files_cmd,
                      'extra_pkg_cmd': (' --packages %s' % ','.join(packages)) if packages is not None else '',
                      'spark-confs': additional_configs,
                      'jars': self.get_jars_list(module_dir, jars_from_lib) + (
                              ',%s/%s.jar' % (module_dir, module)) if include_main_jar else '',
                      'main_py': exec_py_file,
                      'yarn_application_tags': yarn_tags
                  }

        command = TasksInfra.add_command_params(command, command_params, value_wrap=TasksInfra.EXEC_WRAPPERS['python'])
        res = self.run_bash(command).ok
        for artifact_path in additional_artifacts_paths:
            os.remove(artifact_path)
        return res

    def calc_desired_output_partitions(self, base_path):
        print("Calculating partitions for path: " + base_path)
        if self.dry_run:
            print("Avoiding partitions calculation, this is just a dry run! returning -1")
            return -1
        path, size, success_date = self.latest_success_size_for_path(base_path)
        if size is None:
            print("Couldn't find a past valid path for partitions calculation, avoiding calculation")
            return None
        num_partitions = calc_desired_partitions(size)
        print("Number of desired partitions for %s is %d" % (path, num_partitions))
        return num_partitions

    def determine_mr_output_partitions(self, command_params, determine_reduces_by_output, jvm_opts, default_num_reducers=200):
        base_partition_output_key = 'base_partition_output'
        reducers_config_key = TasksInfra.get_mr_partitions_config_key()

        if jvm_opts and reducers_config_key in jvm_opts:
            print("Num reducers set by Ptask")
            return command_params, jvm_opts

        if determine_reduces_by_output:
            jvm_opts = jvm_opts or {}
            if base_partition_output_key not in command_params:
                raise KeyError("Base path for reducers calculation should have been passed!")
            desired_output_partitions = self.calc_desired_output_partitions(command_params[base_partition_output_key])
            if desired_output_partitions is not None:
                jvm_opts[reducers_config_key] = desired_output_partitions
            else:
                jvm_opts[reducers_config_key] = default_num_reducers
            del command_params[base_partition_output_key]
        return command_params, jvm_opts

    def determine_spark_output_partitions(self, command_params, determine_partitions_by_output, spark_configs):
        base_partition_output_key = 'base_partition_output'
        partitions_config_key = TasksInfra.get_spark_partitions_config_key()

        if spark_configs and partitions_config_key in spark_configs:
            print("Num partitions set by Ptask")
            return command_params, spark_configs

        if determine_partitions_by_output:
            spark_configs = spark_configs or {}
            if base_partition_output_key not in command_params:
                raise KeyError("Base path for output partitions calculation should have been passed!")
            desired_output_partitions = self.calc_desired_output_partitions(command_params[base_partition_output_key])
            if desired_output_partitions is not None:
                spark_configs[partitions_config_key] = desired_output_partitions
            del command_params[base_partition_output_key]
        return command_params, spark_configs

    def build_spark_additional_configs(self, named_spark_args, spark_configs):
        additional_configs = ''
        for key, value in self.hadoop_configs.items():
            additional_configs += ' --conf spark.hadoop.%s=%s' % (key, value)
        if spark_configs:
            for key, value in spark_configs.items():
                additional_configs += ' --conf %s=%s' % (key, value)
        if named_spark_args:
            for key, value in named_spark_args.items():
                additional_configs += ' --%s %s' % (key, value)
        if self.should_profile:
            additional_configs += ' --conf "spark.driver.extraJavaOptions=%s"' % JAVA_PROFILER
            additional_configs += ' --conf "spark.executer.extraJavaOptions=%s"' % JAVA_PROFILER
        return additional_configs

    def set_hdfs_replication_factor(self, replication_factor):
        self.jvm_opts['dfs.replication'] = replication_factor

    def consolidate_dir(self, path, io_format=None, codec=None):

        # several sanity checks over the given path
        assert path is not None
        assert type(path) is str

        if io_format is not None:
            if codec is not None:
                command = self.__compose_infra_command('execute ConsolidateDir %s %s %s' % (path, io_format, codec))
            else:
                command = self.__compose_infra_command('execute ConsolidateDir %s %s' % (path, io_format))
        else:
            command = self.__compose_infra_command('execute ConsolidateDir %s' % path)
        self.run_bash(command)

    def consolidate_parquet_dir(self, dir, order_by=None, ignore_bad_input=False, spark_configs=None, output_dir=None):
        tmp_dir = "/tmp/crush/" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + dir
        params = {'src': dir,
                  'dst': tmp_dir,
                  'm': 1,
                  'ord': order_by
                  }

        configs = {
            'spark.yarn.executor.memoryOverhead': '1024',
            'spark.sql.files.ignoreCorruptFiles': str(ignore_bad_input).lower()
        }
        if spark_configs is None:
            spark_configs = {}
        configs.update(spark_configs)
        ret_val = self.run_py_spark(
            app_name="Consolidate:" + dir,
            module='common',
            use_bigdata_defaults=False,
            files=['/etc/hive/conf/hive-site.xml'],
            py_files=[self.execution_dir + '/mobile/python/sw_mobile/apps_common/spark_logger.py'],
            main_py_file='scripts/utils/crush_parquet.py',
            queue='consolidation',
            command_params=params,
            spark_configs=configs,
            named_spark_args={'num-executors': '20', 'driver-memory': '2G', 'executor-memory': '2G'}
        )
        logging.info("Return value from spark-submit: %s" % ret_val)
        if ret_val:
            if directory_exists(tmp_dir) and not self.dry_run:
                final_output_dir = output_dir or dir
                copy_dir_from_path(tmp_dir, final_output_dir)
                self.assert_output_validity(final_output_dir)
                assert get_size(tmp_dir) == get_size(final_output_dir)
                delete_dir(tmp_dir)
            else:
                ret_val = False
        return ret_val

    def write_to_hbase(self, key, table, col_family, col, value, log=True, snowflake_env=None):
        if log:
            print('writing %s to key %s column %s at table %s' % (value, key, '%s:%s' % (col_family, col), table))
        import happybase
        srv = SnowflakeConfig().get_service_name(service_name="hbase")
        conn = happybase.Connection(srv)
        conn.table(table).put(key, {'%s:%s' % (col_family, col): value})
        conn.close()

    def repair_table(self, db, table):
        self.run_bash('hive -e "use %s; msck repair table %s;" 2>&1' % (db, table))

    def repair_tables(self, tables):
        repair_statements = '; '.join(['use %s; msck repair table %s' % (t[0], t[1]) for t in tables])
        bash = 'hive -e "%s" 2>&1' % repair_statements
        self.run_bash(bash)

    # ----------- S3 -----------
    DEFAULT_S3_PROFILE = 'research-safe'

    def read_s3_configuration(self, property_key, section=DEFAULT_S3_PROFILE):
        import boto
        config = boto.pyami.config.Config(path='/etc/aws-conf/.s3cfg')
        return config.get(section, property_key)

    # Storing the credentials in env variables is probably the least secured option and is disabled by default.
    # Please set set_env_variables only as a last resort.
    def set_s3_keys(self, access=None, secret=None, section=DEFAULT_S3_PROFILE, set_env_variables=False):
        access_key = access or self.read_s3_configuration('access_key', section=section)
        self.hadoop_configs['fs.s3a.access.key'] = access_key
        self.run_bash('aws configure set aws_access_key_id %s' % access_key)
        if set_env_variables:
            os.environ["AWS_ACCESS_KEY_ID"] = access_key

        secret_key = secret or self.read_s3_configuration('secret_key', section=section)
        self.hadoop_configs['fs.s3a.secret.key'] = secret_key
        self.run_bash('aws configure set aws_secret_access_key %s' % secret_key)
        if set_env_variables:
                os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key

    def assert_s3_input_validity(self, bucket_name, path, min_size=0, validate_marker=False, profile=DEFAULT_S3_PROFILE, dynamic_min_size=False):
        s3_conn = s3_connection.get_s3_connection(profile=profile)
        bucket_name = bucket_name.replace("/", "")
        bucket = s3_conn.get_bucket(bucket_name)
        ans = True
        min_size = min_size
        if dynamic_min_size:
            min_size = self.get_dynamic_min_dir_size(s3_conn, bucket_name, path)
        if validate_marker:
            ans = ans and validate_success(bucket=bucket, path=path)
        ans = ans and is_s3_folder_big_enough(s3_conn=s3_conn, bucket_name=bucket_name, path=path, min_size=min_size)
        assert ans is True, 'Input is not valid, given bucket is %s and path is %s' % (bucket_name, path)

    def assert_s3_output_validity(self, bucket_name, path, min_size=0, validate_marker=False, profile=DEFAULT_S3_PROFILE, dynamic_min_size=False):
        s3_conn = s3_connection.get_s3_connection(profile=profile)
        bucket_name = bucket_name.replace("/", "")
        bucket = s3_conn.get_bucket(bucket_name)
        ans = True
        min_size = min_size
        if dynamic_min_size:
            min_size = self.get_dynamic_min_dir_size(s3_conn, bucket_name, path)
        if validate_marker:
            ans = ans and validate_success(bucket=bucket, path=path)
        ans = ans and is_s3_folder_big_enough(s3_conn=s3_conn, bucket_name=bucket_name, path=path, min_size=min_size)
        assert ans is True, 'Output is not valid, given bucket is %s and path is %s' % (bucket_name, path)

    def get_dynamic_min_dir_size(self, s3_conn, bucket_name, path, min_std=3, time_delta=10):
        path = path.split('year')[0] # removing the date suffix
        sizes_list = []
        for i in range(1, time_delta+1):
            sizes_list.append(get_s3_folder_size(s3_conn=s3_conn, bucket_name=bucket_name, path=path + self.year_month_before_day(i)))
        sizes_list = [a for a in sizes_list if a != 0]
        if len(sizes_list)>0:
            min_size = np.mean(sizes_list) - (min_std * np.std(sizes_list))
        else:
            min_size = 0
        return max(0,min_size)

    def print_job_input_dict(self, dict):
        print("Job input params: ")
        for key, value in dict.items():
            print("-%s %s" % (key, value))

    @property
    def base_dir(self):
        return self.__get_common_args()['base_dir']

    @property
    def calc_dir(self):
        return self.__get_common_args().get('calc_dir', self.base_dir)

    @property
    def production_base_dir(self):
        return '/similargroup/data'

    @property
    def force(self):
        return self.__get_common_args()['force']

    @property
    def date(self):
        return self.__get_common_args()['date']

    @property
    def mode(self):
        return self.__get_common_args()['mode']

    @property
    def mode_type(self):
        if 'mode_type' in self.__get_common_args():
            return self.__get_common_args()['mode_type']
        if 'mode' in self.__get_common_args():
            mode = self.__get_common_args()['mode']
            default_mode_types = {'snapshot': 'monthly',
                                  'window': 'last-28',
                                  'daily': 'daily',
                                  'mutable': 'mutable'}
            if mode in default_mode_types:
                return default_mode_types[mode]
        raise KeyError('unable to determine mode_type')

    @property
    def interval_delta(self):
        return relativedelta(months=1) if self.mode == 'snapshot' else relativedelta(days=1)

    @property
    def date_suffix(self):
        return self.year_month() if self.mode == 'snapshot' else self.year_month_day()

    @property
    def type_date_suffix(self):
        return 'type=%s/' % self.mode_type + self.date_suffix

    # suffix for hbase tables
    @property
    def table_suffix(self):
        if self.mode == 'snapshot':
            return '_%s' % self.date.strftime('%y_%m')
        elif self.mode == 'daily':
            return '_%s' % self.date.strftime('%y_%m_%d')
        elif self.mode == 'mutable':
            return ''
        else:
            return '_%s_%s' % (self.mode_type, self.date.strftime('%y_%m_%d'))

    @property
    def date_title(self):
        return self.date.strftime('%Y-%m' if self.mode == 'snapshot' else '%Y-%m-%d')

    @property
    def table_prefix(self):
        table_prefix_raw = self.__get_common_args().get('table_prefix', '')
        # Non-empty prefix is enforced to end with an underscore
        if table_prefix_raw and table_prefix_raw[-1] != '_':
            return '%s_' % table_prefix_raw
        else:
            return table_prefix_raw

    @property
    def rerun(self):
        return self.__get_common_args()['rerun']

    @property
    def should_profile(self):
        return self.__get_common_args()['profile']

    @property
    def env_type(self):
        return self.__get_common_args().get('env_type')

    @property
    def dry_run(self):
        return self.__get_common_args()['dry_run']

    @property
    def checks_only(self):
        return self.__get_common_args()['checks_only']

    @property
    def execution_user(self):
        return self.__get_common_args()['execution_user']

    @property
    def task_id(self):
        if self.has_task_id:
            return self.__get_common_args()['task_id']
        else:
            return 'NO_TASK_ID'

    @property
    def dag_id(self):
        return self.__get_common_args()['dag_id']

    @property
    def execution_dt(self):
        return self.__get_common_args()['execution_dt']

    @property
    def has_task_id(self):
        return self.__get_common_args()['has_task_id']

    @property
    def execution_dir(self):
        return self.__get_common_args()['execution_dir']

    @property
    def branch(self):
        return self.__get_common_args().get('branch')
