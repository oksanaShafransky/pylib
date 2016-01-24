import sys
import time
import os
import datetime
import subprocess
import snakebite.client
from snakebite.errors import FileNotFoundException
import types

MRP_HDFS_NAMENODE_PORT = 8020
MRP_HDFS_NAMENODE_SERVER = 'active.hdfs-namenode-mrp.service.production'

# The execution_dir should be a relative path to the project's top-level directory
execution_dir = os.path.dirname(os.path.realpath(__file__))[1:]+"/../.."


class TasksInfra(object):

    @staticmethod
    def parse_date(date_str):
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

    @staticmethod
    def is_valid_output_exists(directory, valid_output_min_size_bytes):
        hdfs_client = snakebite.client.Client(MRP_HDFS_NAMENODE_SERVER, MRP_HDFS_NAMENODE_PORT, use_trash=False)
        is_valid = False
        print 'Checking if a valid output dir %s already exists :' % directory
        try:
            space_consumed = hdfs_client.count([directory]).next()['spaceConsumed']
            is_valid = space_consumed > valid_output_min_size_bytes
            print 'Space consumed by dir is %d' % space_consumed
        except FileNotFoundException:
            print 'Dir does not exist'
        finally:
            if is_valid:
                print 'Output dir already exists'
            return is_valid

    @staticmethod
    def assert_input_validity(directory, valid_input_min_size_bytes):
        hdfs_client = snakebite.client.Client(MRP_HDFS_NAMENODE_SERVER, MRP_HDFS_NAMENODE_PORT, use_trash=False)
        is_valid = False
        print 'Asserting input validity for dir %s :' % directory
        try:
            space_consumed = hdfs_client.count([directory]).next()['spaceConsumed']
            is_valid = space_consumed > valid_input_min_size_bytes
            print 'Space consumed is %d' % space_consumed
        except FileNotFoundException:
            print 'Dir does not exist'
        finally:
            assert is_valid is True, 'Input dir is not valid, given dir is %s' % directory

    @staticmethod
    def assert_output_validity(directory, valid_output_min_size_bytes):
        hdfs_client = snakebite.client.Client(MRP_HDFS_NAMENODE_SERVER, MRP_HDFS_NAMENODE_PORT, use_trash=False)
        is_valid = False
        print 'Asserting output validity for dir %s :' % directory
        try:
            space_consumed = hdfs_client.count([directory]).next()['spaceConsumed']
            is_valid = space_consumed > valid_output_min_size_bytes
            print 'Space consumed is %d' % space_consumed
        except FileNotFoundException:
            print 'Dir does not exist'
        finally:
            assert is_valid is True, 'Output dir is not valid, given dir is %s' % directory

    # needed because the regular client throws an exception when a parent directory doesnt exist either
    @staticmethod
    def directory_exists(dir_name):
        hdfs_client = snakebite.client.Client(MRP_HDFS_NAMENODE_SERVER, MRP_HDFS_NAMENODE_PORT, use_trash=False)

        try:
            return hdfs_client.test(dir_name, directory=True)
        except FileNotFoundException:
            return False

    @staticmethod
    def upload_file_to_hdfs(file_path, target_path):

        if not TasksInfra.directory_exists(target_path):
            mkdir_cmd = 'hadoop fs -mkdir -p %s' % target_path
            subprocess.call(mkdir_cmd.split(' '))

        put_cmd = 'hadoop fs -put %s %s' % (file_path, target_path)
        subprocess.call(put_cmd.split(' '))

    @staticmethod
    def load_common_args_to_ctx(ctx, dry_run, force, base_dir, date):
        d = {'dry_run': dry_run, 'force': force, 'base_dir': base_dir, 'date': date}
        ctx.config['common_args'] = d
        return ContextualizedTasksInfra(ctx)


class ContextualizedTasksInfra(TasksInfra):

    def __init__(self, ctx):
        self.ctx = ctx

    def get_common_args(self):
        return self.ctx.config.config['common_args']

    def compose_infra_command(self, command):
        ans = 'source %s/scripts/common.sh' % execution_dir
        if self.get_common_args()['dry_run']:
            ans += " && setDryRun"
        ans += " && " + command
        return ans

    def compose_hadoop_runner_command(self, task_params):
        ans = self.compose_infra_command('execute hadoopexec %s/mobile mobile.jar com.similargroup.mobile.main.MobileRunner' % execution_dir)
        for key, value in task_params.iteritems():
            if type(value) == types.BooleanType:
                if value==True:
                    ans += " -%s " % key
            else:
                ans += " -%s " % key
                ans += '"%s"' % value if type(value) != types.BooleanType else ""
        return ans

    def run_bash(self, command):
        print ("Running '%s'" % command)
        sys.stdout.flush()
        time.sleep(1)
        self.ctx.run(command)

    def year_month_day(self):
        d = self.get_common_args()['date']
        year_str = str(d.year)[2:]
        return 'year=%s/month=%s/day=%s' % (year_str, str(d.month).zfill(2), str(d.day).zfill(2))
