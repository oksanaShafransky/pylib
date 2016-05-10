import ConfigParser
import calendar
import logging
import os
import re
import sys
import time

import datetime

from hadoop.hdfs_util import create_client, test_size
from redis import StrictRedis as Redis

# The execution_dir should be a relative path to the project's top-level directory
execution_dir = os.path.dirname(os.path.realpath(__file__)).replace('//', '/') + '/../..'


class TasksInfra(object):
    @staticmethod
    def parse_date(date_str):
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

    @staticmethod
    def year_month_day(date):
        year_str = str(date.year)[2:]
        return 'year=%s/month=%s/day=%s' % (year_str, str(date.month).zfill(2), str(date.day).zfill(2))

    @staticmethod
    def year_month_day_country(date, country):
        return '%s/country=%s' % (TasksInfra.year_month_day(date), country)

    @staticmethod
    def year_month(date):
        year_str = str(date.year)[2:]
        return 'year=%s/month=%s' % (year_str, str(date.month).zfill(2))

    @staticmethod
    def add_command_params(command, command_params):
        ans = command
        for key, value in command_params.iteritems():
            if isinstance(value, bool) and value:
                ans += " -%s" % key
            else:
                ans += " -%s %s" % (key, value)
        return ans


class ContextualizedTasksInfra(TasksInfra):
    def __init__(self, ctx):
        self.ctx = ctx
        self.execution_dir = execution_dir

    @staticmethod
    def __compose_infra_command(command):
        ans = 'source %s/scripts/common.sh && %s' % (execution_dir, command)
        return ans

    @staticmethod
    def __with_rerun_root_queue(command):
        return 'setRootQueue reruns && %s' % command

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
        command = self.add_command_params(command, command_params)
        if rerun_root_queue:
            command = self.__with_rerun_root_queue(command)
        return command

    # Todo: move the HDFS-specific code to hadoop.hdfs_util
    @staticmethod
    def __is_dir_contains_success(directory):
        client = create_client()
        expected_success_path = directory + "/_SUCCESS"
        ans = client.test(path=expected_success_path)
        if ans:
            logging.warn(expected_success_path + " contains _SUCCESS file")
        else:
            logging.warn(expected_success_path + " doesn't contain _SUCCESS file")
        return ans

    @staticmethod
    def __is_hdfs_collection_valid(directories,
                                   min_valid_size_bytes=0,
                                   validate_marker=False):
        ans = True
        if isinstance(directories, list):
            for directory in directories:
                ans = ans and ContextualizedTasksInfra.__is_hdfs_collection_valid(directory, min_valid_size_bytes, validate_marker)
        else:
            directory = directories
            if validate_marker:
                ans = ans and ContextualizedTasksInfra.__is_dir_contains_success(directory)
            if min_valid_size_bytes:
                ans = ans and test_size(directory, min_valid_size_bytes)
        return ans

    @staticmethod
    def is_valid_output_exists(directories,
                               valid_output_min_size_bytes=0,
                               validate_marker=False):
        return ContextualizedTasksInfra.__is_hdfs_collection_valid(directories,
                                                     min_valid_size_bytes=valid_output_min_size_bytes,
                                                     validate_marker=validate_marker)

    def __compose_python_runner_command(self, python_executable, command_params):
        command = self.__compose_infra_command('pyexecute %s/%s' % (execution_dir, python_executable))
        command = self.add_command_params(command, command_params)
        return command

    def __get_common_args(self):
        return self.ctx.config.config['sw_common']

    def log_lineage_hdfs(self, directories, direction):
        if self.execution_user != 'Airflow':
            return
        lineage_value_template = \
            '%(execution_user)s.%(dag_id)s.%(task_id)s.%(execution_dt)s::%(direction)s:hdfs::%(directory)s'

        def lineage_value_log_hdfs_collection_template(directory, direction):
            return lineage_value_template % {
                'execution_user': self.execution_user,
                'dag_id': self.dag_id,
                'task_id': self.task_id,
                'execution_dt': self.execution_dt,
                'directory': directory,
                'direction': direction
            }

        client = Redis(host='redis-bigdata.service.production')
        lineage_key = 'LINEAGE_%s' % datetime.date.today().strftime('%y-%m-%d')
        if isinstance(directories, list):
            for directory in directories:
                lineage_value = lineage_value_log_hdfs_collection_template(directory, direction)
                client.rpush(lineage_key, lineage_value)
        else:
            directory = directories
            lineage_value = lineage_value_log_hdfs_collection_template(directory, direction)
            client.rpush(lineage_key, lineage_value)

    def assert_input_validity(self, directories,
                              valid_input_min_size_bytes=0,
                              validate_marker=False):
        self.log_lineage_hdfs(directories, 'input')
        assert TasksInfra.__is_hdfs_collection_valid(directories,
                                                     min_valid_size_bytes=valid_input_min_size_bytes,
                                                     validate_marker=validate_marker) is True, \
            'Input is not valid, given value is %s' % directories

    def assert_output_validity(self, directories,
                               valid_output_min_size_bytes=0,
                               validate_marker=False):
        self.log_lineage_hdfs(directories, 'output')
        assert TasksInfra.__is_hdfs_collection_valid(directories,
                                                     min_valid_size_bytes=valid_output_min_size_bytes,
                                                     validate_marker=validate_marker) is True, \
            'Output is not valid, given value is %s' % directories

    def run_hadoop(self, jar_path, jar_name, main_class, command_params):
        return self.run_bash(
            self.__compose_hadoop_runner_command(jar_path=jar_path,
                                                 jar_name=jar_name,
                                                 main_class=main_class,
                                                 command_params=command_params,
                                                 rerun_root_queue=self.rerun)
        )

    # Todo: Move it to the mobile project
    def run_mobile_hadoop(self, command_params,
                          main_class='com.similargroup.mobile.main.MobileRunner',
                          rerun_root_queue=False):
        return self.run_hadoop(jar_path='mobile',
                               jar_name='mobile.jar',
                               main_class=main_class,
                               command_params=command_params)

    def run_bash(self, command):
        print ("Running '%s'" % command)
        sys.stdout.flush()
        time.sleep(1)
        self.ctx.run(command)

    def run_python(self, python_executable, command_params):
        return self.run_bash(self.__compose_python_runner_command(python_executable, command_params))

    def latest_monthly_success_date(self, directory, month_lookback):
        d = self.__get_common_args()['date']
        command = self.__compose_infra_command('LatestMonthlySuccessDate %s %s %s' % (directory, d, month_lookback))
        return self.run_bash(command=command).strip()

    def year_month_day(self):
        return TasksInfra.year_month_day(self.__get_common_args()['date'])

    ymd = year_month_day

    def year_month_day_country(self, country):
        return TasksInfra.year_month_day_country(self.__get_common_args()['date'], country)

    def year_month(self):
        return TasksInfra.year_month(self.__get_common_args()['date'])

    ym = year_month

    def days_in_range(self):
        end_date = self.__get_common_args()['date']
        mode_type = self.__get_common_args()['mode_type']

        if mode_type == "last-28":
            start_date = end_date - datetime.timedelta(days=27)
        elif mode_type == "monthly":
            # get last day in month
            last = calendar.monthrange(end_date.year, end_date.month)[1]
            end_date = datetime.datetime(end_date.year, end_date.month, last).date()
            start_date = datetime.datetime(end_date.year, end_date.month, 1).date()

        for i in range((end_date - start_date).days + 1):
            yield start_date + datetime.timedelta(days=i)

    # module is either 'mobile' or 'analytics'
    def run_spark(self, main_class, module, queue, app_name, command_params, jars_from_lib=None):
        jar = './mobile.jar' if module == 'mobile' else './analytics.jar'
        jar_path = '%s/%s' % (self.execution_dir, 'mobile' if module == 'mobile' else 'analytics')
        command = 'cd %(jar_path)s;spark-submit' \
                  ' --queue %(queue)s' \
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
        return self.run_bash(command)

    @staticmethod
    def get_jars_list(module_dir, jars_from_lib):
        if jars_from_lib:
            jars_from_lib = map(lambda x: '%s.jar' % x, jars_from_lib)
        else:
            jars_from_lib = os.listdir('%s/lib' % module_dir)
        jars = ','.join(map(lambda x: module_dir + '/lib/' + x, jars_from_lib))
        return jars

    def run_py_spark(self,
                     main_py_file,
                     app_name=None,
                     command_params=None,
                     files=None,
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
            for key, value in spark_configs.iteritems():
                additional_configs += ' --conf %s=%s' % (key, value)
        if named_spark_args:
            for key, value in named_spark_args.iteritems():
                additional_configs += ' --%s %s' % (key, value)

        if use_bigdata_defaults:
            main_py_file = 'python/' + main_py_file
            if not py_files and os.path.exists(module_dir + '/python/' + module + '.py.zip'):
                py_files = [module_dir + '/python/' + module + '.py.zip']

        command = "spark-submit" \
                  " --name '%(app_name)s'" \
                  " --master yarn-cluster" \
                  ' --queue %(queue)s' \
                  " --deploy-mode cluster" \
                  " --jars '%(jars)s'" \
                  " --files '%(files)s'" \
                  " --py-files '%(py-files)s'" \
                  " %(spark-confs)s" \
                  " '%(execution_dir)s/%(main_py)s'" \
                  % {'app_name': app_name if app_name else os.path.basename(main_py_file),
                     'execution_dir': module_dir,
                     'queue': queue,
                     'files': "','".join(files),
                     'py-files': ','.join(py_files),
                     'spark-confs': additional_configs,
                     'jars': self.get_jars_list(module_dir, jars_from_lib),
                     'main_py': main_py_file
                     }

        command = TasksInfra.add_command_params(command, command_params)
        return self.run_bash(command)

    def read_s3_configuration(self, property_key):
        config = ConfigParser.ConfigParser()
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

    def repair_table(self, db, table):
        self.run_bash('hive -e "use %s; msck repair table %s;" 2>&1' % (db, table))

    @property
    def base_dir(self):
        return self.__get_common_args()['base_dir']

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
        return self.__get_common_args()['mode_type']

    @property
    def rerun(self):
        return self.__get_common_args()['rerun']

    @property
    def execution_user(self):
        return self.__get_common_args()['execution_user']

    @property
    def task_id(self):
        return self.__get_common_args()['task_id']

    @property
    def dag_id(self):
        return self.__get_common_args()['dag_id']

    @property
    def execution_dt(self):
        return self.__get_common_args()['execution_dt']
