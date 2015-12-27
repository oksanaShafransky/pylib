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
    def check_input(directory, max_size_bytes):
        hdfs_client = snakebite.client.Client(MRP_HDFS_NAMENODE_SERVER, MRP_HDFS_NAMENODE_PORT, use_trash=False)
        ans = False
        try:
            space_consumed = hdfs_client.count([directory]).next()['spaceConsumed']
            print 'Checking input:'
            ans = space_consumed < max_size_bytes
            print 'Space consumed is %d - returning %s' % (space_consumed, ans)
        except FileNotFoundException:
            ans = True
            print 'Dir does not exist - returning True'
        finally:
            assert ans is True, "Input check failed"

    @staticmethod
    def check_output(directory, min_size_bytes):
        hdfs_client = snakebite.client.Client(MRP_HDFS_NAMENODE_SERVER, MRP_HDFS_NAMENODE_PORT, use_trash=False)
        ans = False
        print 'Checking output:'
        try:
            space_consumed = hdfs_client.count([directory]).next()['spaceConsumed']
            ans = space_consumed > min_size_bytes
            print 'Space consumed is %d - returning %s' % (space_consumed, ans)
        except FileNotFoundException:
            ans = False
            print 'Dir does not exist - returning False'
        finally:
            assert ans is True, "Output check failed"

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
