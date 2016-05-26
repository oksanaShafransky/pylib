# ! /usr/bin/env python
import logging
import os
import subprocess
import random
import calendar
from datetime import *
from os.path import isfile, join
from os import listdir
from urlparse import urlparse

from dateutil.relativedelta import relativedelta
import sys
import signal

GLOBAL_DRYRUN = False  # This is crazy ugly, should allow executor to deploy jars


class ContextFilter(logging.Filter):
    CURRENT_LOG_NUM = 1

    def filter(self, record):
        record.count = self.CURRENT_LOG_NUM
        self.CURRENT_LOG_NUM += 1
        return True


logging.basicConfig(format='%(asctime)s [ %(process)s : %(count)s ] %(filename)s %(levelname)s %(message)s',
                    # datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG,
                    stream=sys.stdout)
logger = logging.getLogger(os.path.basename(__file__))
logger.addFilter(ContextFilter())

MOBILE_ALL_CATEGORY = '\"\"'
UNRANKED = -1


class GracefulShutdownHandler:
    def __init__(self, on_kill, sig=signal.SIGTERM):
        self.on_kill = on_kill
        self.sig = sig

    def __enter__(self):
        self.interrupted = False
        self.released = False

        logger.info('Registering for graceful shutdown hook...')

        self.original_handler = signal.getsignal(self.sig)

        def handler(signum, frame):
            self.release()
            self.interrupted = True

        signal.signal(self.sig, handler)

        return self

    def __exit__(self, type, value, tb):

        logger.info('Unregistering from graceful shutdown hook...')
        self.release()

    def release(self):
        if self.released:
            return False

        try:
            logger.info('SIGINT received, killing task...')
            self.on_kill()
        except:
            pass
        finally:
            pass

        signal.signal(self.sig, self.original_handler)

        self.released = True

        return True


def getPartitionString(mode, mode_type, year, month, day, **kwargs):
    if mode == "window" or mode_type == "weekly":
        partition_parts = "year=%s, month=%02d, day=%02d, type='%s'" % (year, month, day, mode_type)
    elif mode == "daily":
        partition_parts = "year=%s, month=%02d, day=%02d" % (year, month, day)
    else:
        partition_parts = "year=%s, month=%02d, type='%s'" % (year, month, mode_type)

    for key, value in kwargs.iteritems():
        if value:
            partition_parts += ', %s=%s' % (key, value)
        else:
            partition_parts += ', %s' % key

    return '(%s)' % partition_parts


def getDatePartitionString(year, month, day=None, **kwargs):
    if day:
        partition_parts = "year=%s, month=%02d, day=%02d" % (year, month, day)
    else:
        partition_parts = "year=%s, month=%02d" % (year, month)

    for key, value in kwargs.iteritems():
        if value:
            partition_parts += ', %s=%s' % (key, value)
        else:
            partition_parts += ', %s' % key

    return '(%s)' % partition_parts


def getWhereString(table_prefix, mode, mode_type, year, month, day):
    if mode == "window" or mode_type == "weekly":
        return " %(table_prefix)syear=%(year)02d and %(table_prefix)smonth=%(month)02d and %(table_prefix)sday=%(day)02d and %(table_prefix)stype='%(mode_type)s'" % {
            'table_prefix': table_prefix,
            'year': year,
            'month': month,
            'day': day,
            'mode_type': mode_type}
    else:
        return " %(table_prefix)syear=%(year)02d and %(table_prefix)smonth=%(month)02d and %(table_prefix)stype='%(mode_type)s'" % {
            'table_prefix': table_prefix,
            'year': year,
            'month': month,
            'mode_type': mode_type}


def get_monthly_where(year, month, day=None, table_prefix=None):
    params = {'table': table_prefix,
              'year': year,
              'month': month,
              'day': day
              }

    if table_prefix is not None:
        result = '%(table)s.year=%(year)02d AND %(table)s.month=%(month)02d' % params
        if day is not None:
            result += ' AND %(table)s.day=%(day)02d' % params
    else:
        result = 'year=%02d AND month=%02d' % (year, month)
        if day is not None:
            result += ' AND day=%02d' % day
    return result


def get_range_where_clause(year, month, day, mode, mode_type):
    if int(year) < 100:
        end_date = datetime(2000 + int(year), int(month), int(day)).date()
    else:
        end_date = datetime(int(year), int(month), int(day)).date()
    start_date = ""

    if mode_type == "last-1":
        start_date = end_date - timedelta(days=0)
    elif mode_type == "last-7":
        start_date = end_date - timedelta(days=6)
    elif mode_type == "last-28":
        start_date = end_date - timedelta(days=27)
    elif mode_type == "last-30":
        start_date = end_date - timedelta(days=29)
    elif mode_type == "last-90":
        start_date = end_date - timedelta(days=89)
    elif mode_type == "weekly":
        start_date = end_date - timedelta(days=int(end_date.weekday()))
        end_date = start_date + timedelta(days=6)
    elif mode_type == "monthly":
        return ' (year = %02d and month = %02d) ' % (end_date.year % 100, month)
    elif mode_type == "quarterly":
        start_date = datetime(int(year), int(month) - ((int(month) - 1) % 3), 1).date()
        end_date = start_date + timedelta(days=63)
        return '(year=%02d and month <= %02d and month >= %02d' % (
            end_date.year % 100, start_date.month, end_date.month)
    elif mode_type == "annually":
        return " (year = %02d) " % (end_date.year % 100)

    return get_where_between_dates(start_date, end_date)


def get_day_range_where_clause(year, month, day, window_size):
    if int(year) < 100:
        end_date = datetime(2000 + int(year), int(month), int(day)).date()
    else:
        end_date = datetime(int(year), int(month), int(day)).date()

    return get_where_between_dates(end_date - timedelta(days=int(window_size) - 1), end_date)


def get_where_between_dates(start_date, end_date):
    where_clause = ""

    curr_date = start_date
    while True:
        if where_clause != "":
            where_clause += " or "
        where_clause += ' (year=%02d and month=%02d and day=%02d) ' % (
            curr_date.year % 100, curr_date.month, curr_date.day)
        curr_date = curr_date + timedelta(days=1)
        if curr_date > end_date:
            break

    return ' (%s) ' % where_clause


def get_month_range_where_clause(end_date, months_back):
    return ' or '.join(['(year=%02d and month=%02d)' % (parse_date(end_date + relativedelta(months=-x))[:2]) for x in
                        range(0, months_back)])


def deploy_jar(deploy_path, jar_hdfs_location):
    logger.info("copy jars to hdfs, location on hdfs: " + jar_hdfs_location + " from local path: " + deploy_path)
    if GLOBAL_DRYRUN:
        'print dryrun set, not actually deploying'
        return

    subprocess.call(["hadoop", "fs", "-rm", "-r", jar_hdfs_location])
    subprocess.call(["hadoop", "fs", "-mkdir", "-p", jar_hdfs_location])
    subprocess.call(["hadoop", "fs", "-put", deploy_path + "/analytics.jar", jar_hdfs_location + "/analytics.jar"])
    subprocess.call(
        ["hadoop", "fs", "-put", deploy_path + "/lib/common-1.0.jar", jar_hdfs_location + "/common.jar"])


# use ['add jar %s' % jar for jar in detect_jars(...) ]
def detect_jars(deploy_path, lib_path="lib"):
    main_jars = [jar for jar in listdir(deploy_path) if isfile(join(deploy_path, jar)) and jar.endswith('.jar')]
    full_lib_path = join(deploy_path, lib_path)
    lib_jars = [jar for jar in listdir(full_lib_path) if isfile(join(full_lib_path, jar)) and jar.endswith('.jar')]

    return lib_jars + main_jars


def deploy_all_jars(deploy_path, jar_hdfs_location, lib_path="lib"):
    logger.info("copy jars to hdfs, location on hdfs: " + jar_hdfs_location + " from local path: " + deploy_path)
    if GLOBAL_DRYRUN:
        'print dryrun set, not actually deploying'
        return

    main_jars = [jar for jar in listdir(deploy_path) if isfile(join(deploy_path, jar)) and jar.endswith('.jar')]
    full_lib_path = join(deploy_path, lib_path)
    lib_jars = [jar for jar in listdir(full_lib_path) if isfile(join(full_lib_path, jar)) and jar.endswith('.jar')]

    subprocess.call(["hadoop", "fs", "-rm", "-r", jar_hdfs_location])
    subprocess.call(["hadoop", "fs", "-mkdir", "-p", jar_hdfs_location])
    subprocess.call(["bash", "-c", "hadoop fs -put %s/*.jar %s" % (deploy_path, jar_hdfs_location)])
    subprocess.call(["bash", "-c", "hadoop fs -put %s/*.jar %s" % (full_lib_path, jar_hdfs_location)])

    return lib_jars + main_jars


def wait_on_processes(processes):
    for p in processes:
        print p.communicate()


def table_location(table):
    cmd = ['hive', '-e', '"describe formatted %s;"' % table]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate()
    if p.returncode == 0:
        for line in output.split("\n"):
            if "Location:" in line:
                return str(line.split("\t")[1]).strip()
    raise Exception('Cannot find the location for table %s \n stdout[%s] \n stderr[%s]' % (table, output, err))


def hbase_table_name(table):
    cmd = ['hive', '-e', '"describe formatted %s;"' % table]
    output, err = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE).communicate()
    for line in output.split("\n"):
        if "hbase.table.name" in line:
            return line.split("\t")[2]
    raise Exception('Cannot find the name for table %s stdout[%s] stderr[%s]')


def delete_path(path):
    subprocess.call(("hadoop", "fs", "-rm", "-r", path))


def temp_table_cmds_internal(orig_table_name, temp_root):
    table_name = '%s_temp_%s' % (orig_table_name, random.randint(10000, 99999))
    drop_cmd = '\nDROP TABLE IF EXISTS %s;\n' % table_name
    create_cmd = '''\n
                    CREATE EXTERNAL TABLE %(table_name)s
                    LIKE %(orig_table_name)s
                    LOCATION '%(temp_root)s';
                    USE %(db_name)s;
                    MSCK REPAIR TABLE %(short_table_name)s;
                    \n
                ''' % {'table_name': table_name,
                       'orig_table_name': orig_table_name,
                       'temp_root': temp_root,
                       'db_name': table_name.split('.')[0],
                       'short_table_name': table_name.split('.')[-1]}
    return table_name, drop_cmd, create_cmd


def temp_hbase_table_cmds_internal(orig_table_name, full_hbase_table_name):
    table_name = '%s_temp_%s' % (orig_table_name, random.randint(10000, 99999))
    drop_cmd = '\nDROP TABLE IF EXISTS %s;\n' % table_name
    create_cmd = '''\n
                    CREATE EXTERNAL TABLE %(table_name)s
                    LIKE %(orig_table_name)s
                    TBLPROPERTIES("hbase.table.name" = "%(full_hbase_table_name)s");
                    \n
                ''' % {'table_name': table_name,
                       'orig_table_name': orig_table_name,
                       'full_hbase_table_name': full_hbase_table_name}
    return table_name, drop_cmd, create_cmd


def should_create_external_table(orig_table_name, table_loc):
    def __norm_loc(location):
        location = str(location)
        if 'hdfs://' in location:
            location = urlparse(location).path
        return location.rstrip('/')

    table_loc = __norm_loc(table_loc)
    orig_table_loc = __norm_loc(table_location(orig_table_name))

    logger.info("Checking that '%s' != '%s'" % (orig_table_loc, table_loc))
    return orig_table_loc != table_loc


def should_create_external_hbase_table(orig_table_name, hbase_table_name_val):
    table_name = hbase_table_name(orig_table_name)
    return table_name != hbase_table_name_val


def temp_hbase_table_cmds(orig_table_name, hbase_root_table_name, mode, mode_type, date):
    full_hbase_table_name = hbase_root_table_name + hbase_table_suffix(date, mode, mode_type)
    logger.info(
        "Checking whether to create external table %s for HBase table %s:" % (
            orig_table_name, full_hbase_table_name))
    if should_create_external_hbase_table(orig_table_name, full_hbase_table_name):
        logger.info("Writing to an external table with the given name.")
        return temp_hbase_table_cmds_internal(orig_table_name, full_hbase_table_name)
    else:
        logger.info("Writing to the original table.")
        return orig_table_name, '', ''


def temp_table_cmds(orig_table_name, table_location):
    logger.info("Checking whether to create external table %s in location %s:" % (orig_table_name, table_location))
    if should_create_external_table(orig_table_name, table_location):
        logger.info("Writing to temp table in the given location.")
        return temp_table_cmds_internal(orig_table_name, table_location)
    else:
        logger.info("Writing to the original table in place. The location which was passed is being discarded.")
        repair_cmd = 'MSCK REPAIR TABLE %s;\n' % orig_table_name
        return orig_table_name, repair_cmd, ''


def dedent(s):
    return '\n'.join([line.lstrip() for line in s.split('\n') if line.strip()])


def formatted(f):
    return lambda *args, **kwargs: dedent(f(*args, **kwargs))


def deploy_jars(f):
    def invoke(fnc, *args, **kwargs):
        upload_target = '/similargroup/jars/%s/' % datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        jars_to_add = deploy_all_jars(kwargs['deploy_path'], upload_target)
        add_jars_cmd = '\n'.join(['add jar hdfs://%s/%s;' % (upload_target, jar_name) for jar_name in jars_to_add])
        return fnc(jars_to_add=add_jars_cmd, *args, **kwargs)

    return lambda *args, **kwargs: invoke(f, *args, **kwargs)


def parse_date(dt):
    return int(str(dt.year)[-2:]), dt.month, dt.day


def jar_location(branch='master'):
    return "/similargroup/jars/%s/" % branch


def get_date_where(year, month, day=None):
    if day:
        return 'year=%02d and month=%02d and day=%02d ' % (year, month, day)
    else:
        return 'year=%02d and month=%02d ' % (year, month)


def list_days(end_date, mode, mode_type):
    if mode == 'snapshot':
        if mode_type == 'monthly':
            wkday, numdays = calendar.monthrange(end_date.year, end_date.month)
            return [date(year=end_date.year, month=end_date.month, day=x + 1) for x in range(0, numdays)]
        elif mode_type == 'weekly':
            delta = timedelta(weeks=1)
        else:
            return None
    elif mode == 'window':
        if mode_type == 'last-30':
            delta = timedelta(days=30)
        elif mode_type == 'last-28':
            delta = timedelta(days=28)
        elif mode_type == 'last-7':
            delta = timedelta(days=7)
        else:
            return None
    else:
        return None

    return [end_date - timedelta(days=x) for x in range(0, delta.days)]


def hbase_table_suffix(date, mode, mode_type, in_date_fmt='%Y-%m-%d'):
    # in_date_fmt='%Y-%m' if mode == 'snapshot' else '%Y-%m-%d'
    date_fmt = '_%y_%m' if mode == 'snapshot' else '_%y_%m_%d'
    # date_suffix = datetime.strftime(datetime.strptime(date, in_date_fmt), date_fmt)
    date_suffix = datetime.strftime(date, date_fmt)
    return date_suffix if mode == 'snapshot' else '_%s%s' % (mode_type, date_suffix)
