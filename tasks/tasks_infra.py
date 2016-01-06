import sys
import time
import os
import datetime
import snakebite.client
from snakebite.errors import FileNotFoundException

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
            assert is_valid is True, "Input dir is valid"

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
            assert is_valid is True, "Output dir is valid"

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
            ans += " -%s " % key
            ans += '"%s"' % value
        return ans

    def run_bash(self, command):
        print ("Running '%s'" % command)
        sys.stdout.flush()
        time.sleep(1)
        self.ctx.run(command)

    def year_month_day(self):
        d = self.get_common_args()['date']
        year_str = str(d.year)[2:]
        return 'year=%s/month=%s/day=%s' % (year_str, d.month, d.day)
