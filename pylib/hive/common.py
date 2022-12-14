# ! /usr/bin/env python
import calendar
import logging
import os
import random
import signal
import six
import subprocess
import sys
from datetime import *
from dateutil.relativedelta import relativedelta
from os import listdir
from os.path import isfile, join
from six.moves.urllib.parse import urlparse

GLOBAL_DRYRUN = False  # This is crazy ugly, should allow executor to deploy jars

logger = logging.getLogger('hive')

MOBILE_ALL_CATEGORY = '\"\"'
UNRANKED = -1


class GracefulShutdownHandler(object):
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
    if mode == "daily":
        partition_parts = "year=%s, month=%02d, day=%02d" % (year, month, day)
    elif mode == "window" or mode_type == "weekly":
        partition_parts = "year=%s, month=%02d, day=%02d, type='%s'" % (year, month, day, mode_type)
    else:
        partition_parts = "year=%s, month=%02d, type='%s'" % (year, month, mode_type)

    for key, value in six.iteritems(kwargs):
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

    for key, value in six.iteritems(kwargs):
        if value:
            partition_parts += ', %s=%s' % (key, value)
        else:
            partition_parts += ', %s' % key

    return '(%s)' % partition_parts


def get_date_partition_path(year, month, day=None, **kwargs):
    short_year = year % 100
    if day:
        partition_parts = "year=%s/month=%02d/day=%02d" % (short_year, month, day)
    else:
        partition_parts = "year=%s/month=%02d" % (short_year, month)

    for key, value in six.iteritems(kwargs):
        if value:
            partition_parts += '/%s=%s' % (key, value)
        else:
            partition_parts += '/%s' % key

    return '%s' % partition_parts


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


def deploy_all_jars(deploy_path, jar_hdfs_location, lib_path="lib", lib_jars_filter=None):
    logger.info("copy jars to hdfs, location on hdfs: " + jar_hdfs_location + " from local path: " + deploy_path)
    if GLOBAL_DRYRUN:
        'print dryrun set, not actually deploying'
        return

    main_jars = [jar for jar in listdir(deploy_path) if isfile(join(deploy_path, jar)) and jar.endswith('.jar')]

    subprocess.call(["hadoop", "fs", "-rm", "-r", jar_hdfs_location])
    subprocess.call(["hadoop", "fs", "-mkdir", "-p", jar_hdfs_location])
    subprocess.call(["bash", "-c", "hadoop fs -put %s/*.jar %s" % (deploy_path, jar_hdfs_location)])

    full_lib_path = join(deploy_path, lib_path)
    if lib_jars_filter is None:
        lib_jars = [jar for jar in listdir(full_lib_path) if isfile(join(full_lib_path, jar)) and jar.endswith('.jar')]
        subprocess.call(["bash", "-c", "hadoop fs -put %s/*.jar %s" % (full_lib_path, jar_hdfs_location)])
    else:
        lib_jars = [jar for jar in listdir(full_lib_path)
                    if isfile(join(full_lib_path, jar)) and jar.endswith('.jar') and jar in lib_jars_filter]
        for jar in lib_jars:
            subprocess.call(["bash", "-c", "hadoop fs -put %s/%s %s" % (full_lib_path, jar, jar_hdfs_location)])

    return lib_jars + main_jars


def wait_on_processes(processes):
    for p in processes:
        print (p.communicate())


def get_table_location(table):
    cmd = ['hive', '-e', '"describe formatted %s;"' % table]
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as e:
        logger.warn("Unable to run 'hive'. This usually happens in dev scenario. Returning empty string")
        return ''
    output, err = p.communicate()
    if p.returncode == 0:
        for line in output.split("\n"):
            if "Location:" in line:
                return str(line.split("\t")[1]).strip()
    raise Exception('Cannot find the location for table %s \n stdout[%s] \n stderr[%s]' % (table, output, err))


def extract_partitions_by_prefix(prefix, text):
    partitions = []
    for line in text.split("\n"):
        line = line.strip()
        if prefix in line and line.index(prefix) == 0:
            if prefix != line:
                partitions.append(line)
    return partitions


def get_table_partitions_by_prefix(table, prefix):
    cmd = ['hive', '-e', '"show partitions %s;"' % table]
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as e:
        logger.warn("Unable to run 'hive'. This usually happens in dev scenario. Returning empty result")
        return []
    output, err = p.communicate()
    if p.returncode == 0:
        return extract_partitions_by_prefix(prefix, output)
    raise Exception('Cannot find the partitions for table %s \n stdout[%s] \n stderr[%s]' % (table, output, err))


def hbase_table_name(table):
    cmd = ['hive', '-e', '"describe formatted %s;"' % table]
    output, err = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE).communicate()
    for line in output.split("\n"):
        if "hbase.table.name" in line:
            return line.split("\t")[2]
    raise Exception('Cannot find the name for table %s stdout[%s] stderr[%s]' % (table, output, err))


def temp_table_cmds_internal(orig_table_name, temp_root):
    table_name = generate_randomized_table_name(orig_table_name)
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
    table_name = generate_randomized_table_name(orig_table_name)
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


def generate_randomized_table_name(orig_table_name):
    return '%s_temp_%s' % (orig_table_name, random.randint(10000, 99999))


def should_create_temp_table(orig_table_name, table_loc):
    def __normalalize(location):
        return os.path.normpath(urlparse(str(location)).path)

    table_loc = __normalalize(table_loc)
    orig_table_loc = __normalalize(get_table_location(orig_table_name))

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
    logger.info("Checking whether to create temporary table %s over location %s:" % (orig_table_name, table_location))
    if should_create_temp_table(orig_table_name, table_location):
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
        add_jars_cmd = '\n'.join(['add jar %s;' % jar_name for jar_name in detect_local_jars(kwargs['deploy_path'])])
        return add_jars_cmd + fnc(jars_to_add=add_jars_cmd, *args, **kwargs)

    return lambda *args, **kwargs: invoke(f, *args, **kwargs)


def detect_jars(path):
    jars = ['%s/%s' % (path, jar) for jar in listdir(path) if
            isfile(join(path, jar)) and jar.endswith('.jar')]
    return jars


def detect_local_jars(path):
    full_lib_path = join(path, 'lib')
    return detect_jars(full_lib_path) + detect_jars(path)


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


def get_range_where_clause(year, month, day, mode, mode_type, prefix=''):
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
        return ' (%syear = %02d and %smonth = %02d) ' % (prefix, end_date.year % 100, prefix, month)
    elif mode_type == "quarterly":
        start_date = datetime(int(year), int(month) - ((int(month) - 1) % 3), 1).date()
        end_date = start_date + timedelta(days=63)
        return '(%syear=%02d and %smonth <= %02d and %smonth >= %02d' % (
            prefix, end_date.year % 100, prefix, start_date.month, prefix, end_date.month)
    elif mode_type == "annually":
        return " (%syear = %02d) " % (prefix, end_date.year % 100)
    elif mode_type == "exact-date":
        start_date = end_date

    where_clause = ""

    while True:
        if where_clause != "":
            where_clause += " or "
        where_clause += ' (%syear=%02d and %smonth=%02d and %sday=%02d) ' % (
            prefix, start_date.year % 100, prefix, start_date.month, prefix, start_date.day)
        start_date = start_date + timedelta(days=1)
        if start_date > end_date:
            break

    return ' (%s) ' % where_clause
