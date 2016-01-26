import sys
import time
import os
import datetime
import types

from hadoop.hdfs_util import *


# The execution_dir should be a relative path to the project's top-level directory
execution_dir = os.path.dirname(os.path.realpath(__file__)) + '/../..'


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
    def load_common_args_to_ctx(ctx, dry_run, force, base_dir, date):
        d = {'dry_run': dry_run, 'force': force, 'base_dir': base_dir, 'date': date}
        ctx.config['common_args'] = d
        return ContextualizedTasksInfra(ctx)

    @staticmethod
    def add_command_params(command, command_params):
        ans = command
        for key, value in command_params.iteritems():
            if type(value) == types.BooleanType:
                if value==True:
                    ans += " -%s " % key
            else:
                ans += " -%s " % key
                ans += '"%s"' % value if type(value) != types.BooleanType else ""
        return ans



class ContextualizedTasksInfra(TasksInfra):

    def __init__(self, ctx):
        self.ctx = ctx
        self.execution_dir = execution_dir

    def get_common_args(self):
        return self.ctx.config.config['common_args']

    def compose_infra_command(self, command):
        ans = 'source %s/scripts/common.sh' % execution_dir
        if self.get_common_args()['dry_run']:
            ans += " && setDryRun"
        ans += " && " + command
        return ans

    def compose_hadoop_runner_command(self, command_params):
        command = self.compose_infra_command('execute hadoopexec %s/mobile mobile.jar com.similargroup.mobile.main.MobileRunner' % execution_dir)
        command = self.add_command_params(command, command_params)
        return command

    def compose_python_runner_command(self, python_executable, command_params):
        command = self.compose_infra_command('pyexecute %s/%s' % (execution_dir, python_executable))
        command = self.add_command_params(command, command_params)
        return command

    def run_bash(self, command):
        print ("Running '%s'" % command)
        sys.stdout.flush()
        time.sleep(1)
        self.ctx.run(command)

    def year_month_day(self):
        d = self.get_common_args()['date']
        year_str = str(d.year)[2:]
        return 'year=%s/month=%s/day=%s' % (year_str, str(d.month).zfill(2), str(d.day).zfill(2))
