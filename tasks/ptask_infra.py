import ConfigParser
import calendar
import os
import re
import sys
import time

import datetime

from hadoop.hdfs_util import *

# The execution_dir should be a relative path to the project's top-level directory
execution_dir = os.path.dirname(os.path.realpath(__file__)).replace('//', '/') + '/../..'


class TasksInfra(object):
    @staticmethod
    def parse_date(date_str):
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

    #Todo: move the HDFS-specific code to hadoop.hdfs_util
    @staticmethod
    def _assert_dir_contains_success(directory):
        client = create_client()
        expected_success_path = directory + "/_SUCCESS"
        if not client.test(path=expected_success_path):
            raise AssertionError(expected_success_path + " doesn't contain _SUCCESS file")

    @staticmethod
    def is_hdfs_collection_valid(directories,
                                 valid_output_min_size_bytes=0,
                                 validate_marker=False):
        ans = True
        if isinstance(directories, list):
            for directory in directories:
                ans = ans and TasksInfra.is_hdfs_collection_valid(directory, valid_output_min_size_bytes, validate_marker)
        else:
            directory = directories
            if validate_marker:
                ans = ans and TasksInfra._assert_dir_contains_success(directory)
            ans = ans and test_size(directory, valid_output_min_size_bytes)
        return ans

    @staticmethod
    def is_valid_output_exists(directories,
                               valid_output_min_size_bytes=0,
                               validate_marker=False):
        return TasksInfra.is_hdfs_collection_valid(directories,
                                                   valid_output_min_size_bytes,
                                                   validate_marker)

    @staticmethod
    def assert_input_validity(directories,
                              valid_input_min_size_bytes=0,
                              validate_marker=False):
        assert TasksInfra.is_hdfs_collection_valid(directories,
                                                   valid_input_min_size_bytes,
                                                  validate_marker) is True,\
            'Input is not valid, given value is %s' % directories


    @staticmethod
    def assert_output_validity(directories,
                               valid_input_min_size_bytes=0,
                               validate_marker=False):
        assert TasksInfra.is_hdfs_collection_valid(directories,
                                                   valid_input_min_size_bytes,
                                                   validate_marker) is True, \
            'Output is not valid, given value is %s' % directories

    @staticmethod
    def year_month_day(date):
        year_str = str(date.year)[2:]
        return 'year=%s/month=%s/day=%s' % (year_str, str(date.month).zfill(2), str(date.day).zfill(2))

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

    def __compose_python_runner_command(self, python_executable, command_params):
        command = self.__compose_infra_command('pyexecute %s/%s' % (execution_dir, python_executable))
        command = self.add_command_params(command, command_params)
        return command

    def __get_common_args(self):
        return self.ctx.config.config['sw_common']

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

    def year_month(self):
        return TasksInfra.year_month(self.__get_common_args()['date'])

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
                     use_bigdata_defaults=False
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
                  " --deploy-mode cluster" \
                  " --jars '%(jars)s'" \
                  " --files '%(files)s'" \
                  " --py-files '%(py-files)s'" \
                  " %(spark-confs)s" \
                  " '%(execution_dir)s/%(main_py)s'" \
                  % {'app_name': app_name if app_name else os.path.basename(main_py_file),
                     'execution_dir': module_dir,
                     'files': "','".join(files),
                     'py-files': ','.join(py_files),
                     'spark-confs': additional_configs,
                     'jars': self.get_jars_list(module_dir, jars_from_lib),
                     'main_py': main_py_file
                     }

        command = TasksInfra.add_command_params(command, command_params)
        return self.run_bash(command)

    def read_s3_configuration(self, property):
        config = ConfigParser.ConfigParser()
        config.read('%s/scripts/.s3cfg' % self.execution_dir)
        return config.get('default', property)

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
    def rerun(self):
        return self.__get_common_args()['rerun']


def logged(func):
    def logging_wrapper(*args, **kwargs):
        task_name = func.__name__
        print 'Starting %s' % task_name
        print "Arguments are: %s, %s" % (args, kwargs)
        retval = func(*args, **kwargs)
        print '%s is finished' % task_name
        return retval

    return logging_wrapper
