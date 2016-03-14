import sys
import time
import os
import datetime
import types
import ConfigParser
from calendar import monthrange

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
    def is_output_successful(directory):
        return test_size('%s/%s' % (directory, '_SUCCESS'), 0)

    @staticmethod
    def is_valid_output_exists(directory, valid_output_min_size_bytes):
        return test_size(directory, valid_output_min_size_bytes)

    @staticmethod
    def assert_input_validity(directories, valid_input_min_size_bytes):
        if isinstance([], directories):
            for dir in directories:
                assert_input_validity(dir, valid_input_min_size_bytes)


        assert test_size(directory, valid_input_min_size_bytes) is True, 'Input is not valid, given value is %s' % directory

    @staticmethod
    def assert_output_validity(directory, valid_output_min_size_bytes):
        assert test_size(directory, valid_output_min_size_bytes) is True, 'Output is not valid, given value is %s' % directory

    @staticmethod
    def assert_output_success(directory):
        assert test_size('%s/%s' % (directory, '_SUCCESS'), 0) is True, 'Output is not valid, given value is %s' % directory


    @staticmethod
    def load_common_args_to_ctx(ctx, dry_run, force, base_dir, date, mode, mode_type):
        d = {'dry_run': dry_run, 'force': force, 'base_dir': base_dir, 'date': date, 'mode': mode, 'mode_type': mode_type}
        ctx.config['common_args'] = d
        return ContextualizedTasksInfra(ctx)

    @staticmethod
    def add_command_params(command, command_params):
        ans = command
        for key, value in command_params.iteritems():
            if type(value) == types.BooleanType:
                if value == True:
                    ans += " -%s " % key
            else:
                ans += " -%s " % key
                ans += '"%s"' % value if type(value) != types.BooleanType else ""
        return ans


class ContextualizedTasksInfra(TasksInfra):

    def __init__(self, ctx):
        self.ctx = ctx
        self.execution_dir = execution_dir

    def __compose_infra_command(self, command):
        ans = 'source %s/scripts/common.sh' % execution_dir
        if self.__get_common_args()['dry_run']:
            ans += " && setDryRun"
        ans += " && " + command
        return ans

    def __compose_hadoop_runner_command(self, jar_path, jar_name, main_class, command_params):
        command = self.__compose_infra_command('execute hadoopexec %(base_dir)s/%(jar_relative_path)s %(jar)s %(class)s' %
                                               {
                                                     'base_dir': execution_dir,
                                                     'jar_relative_path': jar_path,
                                                     'jar': jar_name,
                                                     'class': main_class
                                                 }
                                               )
        command = self.add_command_params(command, command_params)
        return command

    #Todo: Move it to the mobile project
    def __compose_mobile_hadoop_runner_command(self, command_params):
        return self.__compose_hadoop_runner_command(jar_path='mobile', jar_name='mobile.jar', main_class='com.similargroup.mobile.main.MobileRunner', command_params=command_params)

    def __compose_python_runner_command(self, python_executable, command_params):
        command = self.__compose_infra_command('pyexecute %s/%s' % (execution_dir, python_executable))
        command = self.add_command_params(command, command_params)
        return command

    def __get_common_args(self):
        return self.ctx.config.config['common_args']

    #Todo: Move it to the mobile project
    def run_mobile_hadoop(self, command_params):
        return self.run_bash(self.__compose_mobile_hadoop_runner_command(command_params))

    def run_hadoop(self, jar_path, jar_name, main_class, command_params):
        return self.run_bash(
                self.__compose_hadoop_runner_command(jar_path=jar_path, jar_name=jar_name, main_class=main_class, command_params=command_params)
        )

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

    def year_month(self):
        d = self.__get_common_args()['date']
        year_str = str(d.year)[2:]
        return 'year=%s/month=%s' % (year_str, str(d.month).zfill(2))

    def days_in_range(self):
        d = self.__get_common_args()['date']
        year_str = str(d.year)[2:]
        return 'year=%s/month=%s' % (year_str, str(d.month).zfill(2))



    # module is either 'mobile' or 'analytics'
    def run_spark(self, main_class, module, queue, app_name, command_params, jars_from_lib=None):
        jar = './mobile.jar' if module == 'mobile' else './analytics.jar'
        jar_path = '%s/%s' % (self.execution_dir, 'mobile' if module == 'mobile' else 'analytics')
        if jars_from_lib is None:
            jars_from_lib = os.listdir('%s/lib' % (jar_path))
        else:
            jars_from_lib = map(lambda x: '%s.jar' % x, jars_from_lib)
        jars = ','.join(map(lambda x: './lib/%s'%x, jars_from_lib))
        command = 'cd %s;spark-submit --queue %s --name "%s" --master yarn-cluster --deploy-mode cluster --jars %s --class %s %s ' % \
                  (jar_path, queue, app_name, jars, main_class, jar)
        command = TasksInfra.add_command_params(command,command_params)
        return self.run_bash(command)

    #TODO: handle additional configs, execution dir
    def run_py_spark(self, files, py_files, main_py_file, command_params, spark_confgis, **kwargs):
        additional_configs = ''

        for key, value in spark_confgis.iteritems():
            additional_configs += ' --conf %s=%s' % (key, value)

        for key, value in kwargs.iteritems():
            additional_configs += ' --%s %s' % (key, value)

        command = "cd %s/mobile;spark-submit --master yarn-cluster --files %s --py-files %s %s %s " % \
                  (self.execution_dir, ','.join(files), ','.join(py_files), additional_configs, main_py_file)
        command = TasksInfra.add_command_params(command,command_params)
        return self.run_bash(command)

    def read_s3_configuration(self, property):
        config = ConfigParser.ConfigParser()
        config.read('%s/scripts/.s3cfg' % self.execution_dir)
        return config.get('default', property)
