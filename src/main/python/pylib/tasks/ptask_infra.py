from six.moves import configparser
import calendar
import datetime
import os
import re
import six
import sys
import time
import shutil
import logging

# Adjust log level
logging.getLogger("urllib3").setLevel(logging.WARNING)

from invoke import Result
from invoke.exceptions import Failure
from redis import StrictRedis as Redis

from pylib.hive.hive_runner import HiveProcessRunner, HiveParamBuilder
from pylib.hive.common import random_str
from pylib.hadoop.hdfs_util import test_size, check_success, mark_success, delete_dirs, get_file
from pylib.sw_config.kv_factory import provider_from_config

# The execution_dir should be a relative path to the project's top-level directory
execution_dir = os.path.dirname(os.path.realpath(__file__)).replace('//', '/') + '/../../../..'


class KeyValueProvider(object):
    conf = """{
             "pylib.sw_config.consul.ConsulProxy": {
                 "server":"consul.service.production"
             },
             "pylib.sw_config.etcd_kv.EtcdProxy": {
                 "server":"etcd.service.production",
                 "port": 4001,
                 "root_path": "v1/production"
             }
             }"""
    conf = provider_from_config(conf)

    @staticmethod
    def get(key):
        return KeyValueProvider.conf.get(key)

    @staticmethod
    def set(key, value):
        return KeyValueProvider.conf.set(key, value)

    @staticmethod
    def delete(key):
        return KeyValueProvider.conf.delete(key)

    @staticmethod
    def subkeys(key):
        return KeyValueProvider.conf.sub_keys(key)


class TasksInfra(object):
    kv = KeyValueProvider()

    @staticmethod
    def parse_date(date_str, fmt='%Y-%m-%d'):
        return datetime.datetime.strptime(date_str, fmt).date()

    @staticmethod
    def latest_monthly_success_date_kv(base_path):
        return TasksInfra.__latest_success_date_kv(base_path, fmt='%Y-%m')

    @staticmethod
    def latest_daily_success_date_kv(base_path):
        return TasksInfra.__latest_success_date_kv(base_path, fmt='%Y-%m-%d')

    @staticmethod
    def __latest_success_date_kv(base_path, fmt):
        dates = sorted(TasksInfra.kv.subkeys(base_path), reverse=True)
        for date in dates:
            if TasksInfra.kv.get('%s/%s' % (base_path, date)) == 'success':
                return TasksInfra.parse_date(date, fmt)

        return None

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
    def year_month_day(date):
        if date is None:
            raise AttributeError("date wasn't passed")
        year_str = str(date.year)[2:]
        return 'year=%s/month=%s/day=%s' % (year_str, str(date.month).zfill(2), str(date.day).zfill(2))

    @staticmethod
    def year_month_day_country(date, country):
        return '%s/country=%s' % (TasksInfra.year_month_day(date), country)

    @staticmethod
    def year_month(date):
        if date is None:
            raise AttributeError("date wasn't passed")
        year_str = str(date.year)[2:]
        return 'year=%s/month=%s' % (year_str, str(date.month).zfill(2))

    @staticmethod
    def year_month_country(date, country):
        return '%s/country=%s' % (TasksInfra.year_month(date), country)

    @staticmethod
    def days_in_range(end_date, mode_type):
        if mode_type == 'last-28':
            start_date = end_date - datetime.timedelta(days=27)
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
    def add_command_params(command, command_params, *positional):
        ans = command + ' ' + ' '.join(positional)

        for key, value in command_params.items():
            if value is None:
                continue
            if isinstance(value, bool):
                if value:
                    ans += " -%s" % key
            elif isinstance(value, list):
                for elem in value:
                    ans += " -%s '%s'" % (key, elem)
            else:
                ans += " -%s '%s'" % (key, value)
        return ans


class ContextualizedTasksInfra(object):
    def __init__(self, ctx):
        """
        :param ctx: invoke.context.Context
        """
        self.ctx = ctx
        self.execution_dir = execution_dir

    @staticmethod
    def __compose_infra_command(command):
        ans = 'source %s/scripts/common.sh && %s' % (execution_dir, command)
        return ans

    @staticmethod
    def __with_rerun_root_queue(command):
        return 'source %s/scripts/common.sh && setRootQueue reruns && %s' % (execution_dir, command)

    def __compose_hadoop_runner_command(self, jar_path, jar_name, main_class, command_params, rerun_root_queue=False):
        command = self.__compose_infra_command(
            'execute hadoopexec %(base_dir)s/%(jar_relative_path)s %(jar)s %(class)s' %
            {
                'base_dir': execution_dir,
                'jar_relative_path': jar_path,
                'jar': jar_name,
                'class': main_class
            }
        )
        command = TasksInfra.add_command_params(command, command_params)
        if rerun_root_queue:
            command = self.__with_rerun_root_queue(command)
        return command

    def __is_hdfs_collection_valid(self, directories, min_size_bytes=0, validate_marker=False):
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
                ans = ans and test_size(directory, min_size_bytes)
        return ans

    def is_valid_output_exists(self, directories, min_size_bytes=0, validate_marker=False):
        self.log_lineage_hdfs(directories, 'output')
        return self.__is_hdfs_collection_valid(directories, min_size_bytes, validate_marker)

    def __compose_python_runner_command(self, python_executable, command_params, *positional):
        command = self.__compose_infra_command('pyexecute %s/%s' % (execution_dir, python_executable))
        command = TasksInfra.add_command_params(command, command_params, *positional)
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

        client = Redis(host='redis-bigdata.service.production')
        lineage_key = 'LINEAGE_%s' % datetime.date.today().strftime('%y-%m-%d')
        if isinstance(directories, six.string_types):
            directories = [directories]

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
                client.rpush(lineage_key, lineage_value)

    def assert_input_validity(self, directories, min_size_bytes=0, validate_marker=False):
        self.log_lineage_hdfs(directories, 'input')
        assert self.__is_hdfs_collection_valid(directories,
                                               min_size_bytes=min_size_bytes,
                                               validate_marker=validate_marker) is True, \
            'Input is not valid, given value is %s' % directories

    def assert_output_validity(self, directories,
                               min_size_bytes=0,
                               validate_marker=False):
        self.log_lineage_hdfs(directories, 'output')
        assert self.__is_hdfs_collection_valid(directories,
                                               min_size_bytes=min_size_bytes,
                                               validate_marker=validate_marker) is True, \
            'Output is not valid, given value is %s' % directories

    def run_hadoop(self, jar_path, jar_name, main_class, command_params):
        return self.run_bash(
            self.__compose_hadoop_runner_command(jar_path=jar_path,
                                                 jar_name=jar_name,
                                                 main_class=main_class,
                                                 command_params=command_params,
                                                 rerun_root_queue=self.rerun)
        ).ok

    # managed_output_dirs - dirs to be deleted on start and then marked upon a successful conclusion
    def run_hive(self, query, hive_params=HiveParamBuilder(), query_name='query', partitions=32, query_name_suffix=None,
                 managed_output_dirs=None, cache_files=None, **extra_hive_conf):
        if managed_output_dirs is None:
            managed_output_dirs = []
        if self.rerun:
            hive_params = hive_params.as_rerun()

        job_name = 'Hive. %s - %s - %s' % (query_name, self.date_title, self.mode)
        if query_name_suffix is not None:
            job_name = job_name + ' ' + query_name_suffix

        # delete output on start
        if not self.dry_run:
            delete_dirs(*managed_output_dirs)

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
                                      is_dry_run=self.dry_run)
        for mdir in managed_output_dirs:
            mark_success(mdir)

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
                          rerun_root_queue=False):
        return self.run_hadoop(jar_path='mobile',
                               jar_name='mobile.jar',
                               main_class=main_class,
                               command_params=command_params)

    def run_analytics_hadoop(self, command_params, main_class):
        return self.run_hadoop(jar_path='analytics',
                               jar_name='analytics.jar',
                               main_class=main_class,
                               command_params=command_params)

    def run_bash(self, command):
        sys.stdout.write("#####\nFinal bash command: \n-----------------\n%s\n#####\n" % command)
        sys.stdout.flush()
        time.sleep(1)
        if self.dry_run or self.checks_only:
            return Result(command, stdout=None, stderr=None, exited=0, pty=None)
        return self.ctx.run(command.replace('\'', '\\"\'\\"'))

    def run_python(self, python_executable, command_params, *positional):
        return self.run_bash(self.__compose_python_runner_command(python_executable, command_params, *positional)).ok

    def run_r(self, r_executable, command_params):
        return self.run_bash(self.__compose_infra_command(
            "execute Rscript %s/%s %s" % (execution_dir, r_executable, ' '.join(command_params)))).ok

    def latest_monthly_success_date(self, directory, month_lookback, date=None):
        d = date.strftime('%Y-%m-%d') if date is not None else self.__get_common_args()['date']
        command = self.__compose_infra_command('LatestMonthlySuccessDate %s %s %s' % (directory, d, month_lookback))
        try:
            return self.run_bash(command=command).stdout.strip()
        except Failure:
            return None

    def latest_daily_success_date(self, directory, month_lookback, date=None):
        d = date.strftime('%Y-%m-%d') if date is not None else self.__get_common_args()['date']
        command = self.__compose_infra_command('LatestDailySuccessDate %s %s %s' % (directory, d, month_lookback))
        try:
            return self.run_bash(command=command).stdout.strip()
        except Failure:
            return None

    def mark_success(self, directory, opts=''):
        if self.dry_run or self.checks_only:
            sys.stdout.write('''Dry Run: If successful would create '%s/_SUCCESS' marker\n''' % directory)
        else:
            mark_success(directory)

    def full_partition_path(self):
        return TasksInfra.full_partition_path(self.__get_common_args()['mode'], self.__get_common_args()['mode_type'],
                                              self.__get_common_args()['date'])

    def year_month_day(self, date=None):
        return TasksInfra.year_month_day(self.__get_common_args()['date'] if date is None else date)

    ymd = year_month_day

    def year_month_day_country(self, country):
        return TasksInfra.year_month_day_country(self.__get_common_args()['date'], country)

    def year_month_country(self, country):
        return TasksInfra.year_month_country(self.__get_common_args()['date'], country)

    def year_month(self):
        return TasksInfra.year_month(self.__get_common_args()['date'])

    ym = year_month

    def days_in_range(self):
        end_date = self.__get_common_args()['date']
        mode_type = self.__get_common_args()['mode_type']

        return TasksInfra.days_in_range(end_date, mode_type)

    # module is either 'mobile' or 'analytics'
    def run_spark(self, main_class, module, queue, app_name, command_params, jars_from_lib=None):
        jar = './mobile.jar' if module == 'mobile' else './analytics.jar'
        jar_path = '%s/%s' % (self.execution_dir, 'mobile' if module == 'mobile' else 'analytics')
        command = 'cd %(jar_path)s;spark-submit' \
                  ' --queue %(queue)s' \
                  ' --conf "spark.yarn.tags=$TASK_ID"' \
                  ' --name "%(app_name)s"' \
                  ' --master yarn-cluster' \
                  ' --deploy-mode cluster' \
                  ' --jars %(jars)s' \
                  ' --class %(main_class)s' \
                  ' %(jar)s ' % \
                  {'jar_path': jar_path,
                   'queue': queue,
                   'app_name': app_name,
                   'jars': self.get_jars_list(jar_path, jars_from_lib),
                   'main_class': main_class,
                   'jar': jar}
        command = TasksInfra.add_command_params(command, command_params)
        return self.run_bash(command).ok

    def get_jars_list(self, module_dir, jars_from_lib):
        if jars_from_lib:
            jars_from_lib = map(lambda x: '%s.jar' % x, jars_from_lib)
        else:
            lib_module_dir = '%s/lib' % module_dir
            if self.dry_run or self.checks_only:
                print('Dry Run: Would attach jars from ' + lib_module_dir)
                jars_from_lib = []
            else:
                jars_from_lib = os.listdir(lib_module_dir)
        jars = ','.join(map(lambda x: module_dir + '/lib/' + x, jars_from_lib))
        return jars

    def run_py_spark(self,
                     main_py_file,
                     app_name=None,
                     command_params=None,
                     files=None,
                     include_main_jar=True,
                     jars_from_lib=None,
                     module='mobile',
                     named_spark_args=None,
                     py_files=None,
                     spark_configs=None,
                     use_bigdata_defaults=False,
                     queue=None
                     ):
        if files is None:
            files = []
        additional_configs = ''
        module_dir = self.execution_dir + '/' + module

        if spark_configs:
            for key, value in spark_configs.items():
                additional_configs += ' --conf %s=%s' % (key, value)
        if named_spark_args:
            for key, value in named_spark_args.items():
                additional_configs += ' --%s %s' % (key, value)

        if use_bigdata_defaults:
            main_py_file = 'python/sw_%s/%s' % (module, main_py_file)
            module_source_egg_path = '%s/sw_%s-0.0.0.dev0-py2.7.egg' % (module_dir, module)
            if not py_files and os.path.exists(module_source_egg_path):
                py_files = [module_source_egg_path]
        if py_files is None:
            py_files = []

        if (py_files == []):
            py_files_cmd = ' '
        else:
            py_files_cmd = ' --py-files "%s"' % ','.join(py_files)

        command = 'spark-submit' \
                  ' --name "%(app_name)s"' \
                  ' --master yarn-cluster' \
                  ' --queue %(queue)s' \
                  ' --conf "spark.yarn.tags=$TASK_ID"' \
                  ' --deploy-mode cluster' \
                  ' --jars "%(jars)s"' \
                  ' --files "%(files)s"' \
                  ' %(py_files_cmd)s' \
                  ' %(spark-confs)s' \
                  ' "%(execution_dir)s/%(main_py)s"' \
                  % {'app_name': app_name if app_name else os.path.basename(main_py_file),
                     'execution_dir': module_dir,
                     'queue': queue,
                     'files': "','".join(files),
                     'py_files_cmd': py_files_cmd,
                     'spark-confs': additional_configs,
                     'jars': self.get_jars_list(module_dir, jars_from_lib) + (
                         ',%s/%s.jar' % (module_dir, module)) if include_main_jar else '',
                     'main_py': main_py_file
                     }

        command = TasksInfra.add_command_params(command, command_params)
        return self.run_bash(command).ok

    def read_s3_configuration(self, property_key):
        config = configparser.ConfigParser()
        config.read('%s/scripts/.s3cfg' % self.execution_dir)
        return config.get('default', property_key)

    def consolidate_dir(self, path, io_format=None, codec=None):

        # several sanity checks over the given path
        assert path is not None
        assert type(path) is str
        p1 = re.compile('\/similargroup\/data/analytics\/.+')
        p2 = re.compile('\/similargroup\/data/mobile-analytics\/.+')
        p3 = re.compile('\/similargroup\/data/ios-analytics\/.+')
        p4 = re.compile('\/user\/.+\/.+')
        assert p1.match(path) is not None or p2.match(path) is not None or p3.match(path) is not None or p4.match(
            path) is not None

        if io_format is not None:
            if codec is not None:
                command = self.__compose_infra_command('execute ConsolidateDir %s %s %s' % (path, io_format, codec))
            else:
                command = self.__compose_infra_command('execute ConsolidateDir %s %s' % (path, io_format))
        else:
            command = self.__compose_infra_command('execute ConsolidateDir %s' % path)
        self.run_bash(command)

    def write_to_hbase(self, key, table, col_family, col, value, log=True):
        if log:
            print('writing %s to key %s column %s at table %s' % (value, key, '%s:%s' % (col_family, col), table))
        import happybase
        HBASE = 'mrp'  # TODO: allow for inference based on config
        srv = 'hbase-%s.service.production' % HBASE
        conn = happybase.Connection(srv)
        conn.table(table).put(key, {'%s:%s' % (col_family, col): value})
        conn.close()

    def repair_table(self, db, table):
        self.run_bash('hive -e "use %s; msck repair table %s;" 2>&1' % (db, table))

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
                                  'daily': 'daily'}
            if mode in default_mode_types:
                return default_mode_types[mode]
        raise KeyError('unable to determine mode_type')

    @property
    def date_suffix(self):
        return self.year_month() if self.mode == 'snapshot' else self.year_month_day()

    @property
    def type_date_suffix(self):
        return 'type=%s/' % self.mode_type + self.date_suffix

    @property
    def table_suffix(self):
        if self.mode == 'snapshot':
            return '_%s' % self.date.strftime('%y_%m')
        else:
            return '_%s_%s' % (self.mode_type, self.date.strftime('%y_%m_%d'))

    @property
    def date_title(self):
        return self.date.strftime('%Y-%m' if self.mode == 'snapshot' else '%Y-%m-%d')

    @property
    def table_prefix(self):
        return self.__get_common_args().get('table_prefix', '')

    @property
    def rerun(self):
        return self.__get_common_args()['rerun']

    @property
    def env_type(self):
        return self.__get_common_args()['env_type']

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

    if __name__ == '__main__':
        command = "source /home/felixv/cdh5/pylib/tasks/../../scripts/common.sh && execute hadoopexec /home/felixv/cdh5/pylib/tasks/../../analytics analytics.jar com.similargroup.common.utils.SqlExportUtil  -cs jdbc:mysql://mysql-ga.vip.sg.internal:3306/swga?user=swga\&password=swga\!23 -q 'select domain, country as country_name, deviceId, users, visits from websites_countries_data where year=2016 and month=6 and day=1' -ot parquet -out /similargroup/ga/daily/website-data/year=16/month=06/day=01"
        special_chars = {'\\': '\\', '\'': '\"'}
        for chr, replacement in special_chars.items():
            command = command.replace(chr, '\"%s\"' % replacement)

        print(command)
