import sys
import time
import os
import datetime
import types
import ConfigParser
import re

from hadoop.hdfs_util import *


# The execution_dir should be a relative path to the project's top-level directory
execution_dir = os.path.dirname(os.path.realpath(__file__)).replace('//', '/') + '/../..'


class TasksInfra(object):

    @staticmethod
    def parse_date(date_str):
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

    @staticmethod
    def is_valid_output_exists(directory, valid_output_min_size_bytes):
        return test_size(directory, valid_output_min_size_bytes)

    @staticmethod
    def assert_input_validity(directory, valid_input_min_size_bytes):
        assert test_size(directory, valid_input_min_size_bytes) is True, 'Input dir is not valid, given dir is %s' % directory

    @staticmethod
    def assert_output_validity(directory, valid_output_min_size_bytes):
        assert test_size(directory, valid_output_min_size_bytes) is True, 'Output dir is not valid, given dir is %s' % directory

    @staticmethod
    def add_command_params(command, command_params):
        ans = command
        for key, value in command_params.iteritems():
            if isinstance(value, bool) and value:
                ans += " -%s " % key
            else:
                ans += " -%s " % key
                ans += '"%s"' % value if isinstance(value, bool) else ""
        return ans

    @staticmethod
    def __compose_infra_command(command):
        ans = 'source %s/scripts/common.sh && %s' % (execution_dir, command)
        return ans

    @staticmethod
    def __with_rerun_root_queue(command):
        return 'setRootQueue reruns && %s' % command


class ContextualizedTasksInfra(TasksInfra):

    def __init__(self, ctx):
        self.ctx = ctx
        self.execution_dir = execution_dir

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

    def year_month_day(self):
        d = self.__get_common_args()['date']
        year_str = str(d.year)[2:]
        return 'year=%s/month=%s/day=%s' % (year_str, str(d.month).zfill(2), str(d.day).zfill(2))

    # module is either 'mobile' or 'analytics'
    def run_spark(self, main_class, module, queue, app_name, command_params, jars_from_lib=None):
        jar = './mobile.jar' if module == 'mobile' else './analytics.jar'
        jar_path = '%s/%s' % (self.execution_dir, 'mobile' if module == 'mobile' else 'analytics')
        if jars_from_lib is None:
            jars_from_lib = os.listdir('%s/lib' % jar_path)
        else:
            jars_from_lib = map(lambda x: '%s.jar' % x, jars_from_lib)
        jars = ','.join(map(lambda x: './lib/%s'%x, jars_from_lib))
        command = 'cd %s;spark-submit --queue %s --name "%s" --master yarn-cluster --deploy-mode cluster --jars %s --class %s %s ' % \
                  (jar_path, queue, app_name, jars, main_class, jar)
        command = TasksInfra.add_command_params(command,command_params)
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
        assert p1.match(path) is not None or p2.match(path) is not None or p3.match(path) is not None or p4.match(path) is not None

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