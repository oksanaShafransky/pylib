__author__ = 'Felix'

import subprocess
import random
import pyhs2
import getpass
from datetime import datetime
from dateutil import parser
from urlparse import urlparse
from inspect import isfunction


def trim(s):
    return s.strip(' \t\r\n')

HIVE_SERVER = 'hive-server2-mrp.service.production'
hive_conn = pyhs2.connect(HIVE_SERVER, authMechanism='PLAIN', user=getpass.getuser())


def get_databases():
    with hive_conn.cursor() as curr:
        curr.execute('show databases')
        return [item[0] for item in curr.fetch()]


def get_tables(db):
    with hive_conn.cursor() as curr:
        curr.execute('use %s' % db)
        curr.execute('show tables')
        return [item[0] for item in curr.fetch()]


def repair_table(table_name):
        with hive_conn.cursor() as curr:
            curr.execute('msck repair table %s' % table_name)


def get_table_partitions(table_name):
        with hive_conn.cursor() as curr:
            curr.execute('show partitions %s' % table_name)
            return [dict([fld.split('=') for fld in part_def]) for part_def in [partition[0].split('/') for partition in curr.fetch()]]


def get_table_dates(table_name):
    return [datetime.strptime('%02d-%02d-%02d' % (int(partition['year']) % 100, int(partition['month']), int(partition.get('day', 1))), '%y-%m-%d') for
            partition in get_table_partitions(table_name)
            ]


def create_temp_table(original_table_name, cloned_table_name, location):
    with hive_conn.cursor() as curr:
        curr.execute('create external table %s like %s location \'%s\'' % (cloned_table_name, original_table_name, location))


def get_table_info(table_name):
    with hive_conn.cursor() as curr:
        curr.execute('desc formatted %s' % table_name)
        return list(curr.fetch())


def is_external_table(table_name):
    is_ext_info = [line for line in get_table_info(table_name) if trim(line[0]) == 'Table Type:'][0]
    return trim(is_ext_info[1]).lower() == 'external_table'


def get_table_create_time(table_name):
    create_time_info = [line for line in get_table_info(table_name) if trim(line[0]) == 'CreateTime:'][0]
    return parser.parse(create_time_info[1])


def delete_table(table_name):
    with hive_conn.cursor() as curr:
        curr.execute('drop table %s' % table_name)


def table_location(table):
    cmd = ['hive', '-e', '"describe formatted %s;"' % table]
    output, _ = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE).communicate()
    for line in output.split('\n'):
        if 'Location:' in line:
            return line.split('\t')[1]


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


def should_create_external_table(orig_table_name, location):
    # Remove hdfs:// where needed for comparison
    if 'hdfs://' in location:
        location = urlparse(location).path
    table_loc = table_location(orig_table_name)
    table_loc = urlparse(table_loc).path
    return table_loc != location


# returns (table_to_use, pre_operations, post_operations)
def temp_table_cmds(orig_table_name, root):
    print 'Checking whether to create external table %s in location %s:' % (orig_table_name, root)
    if root is not None and should_create_external_table(orig_table_name, root):
        logger.info('Writing to an external table in the given location.')
        table_name, drop_cmd, create_cmd = temp_table_cmds_internal(orig_table_name, root)
        return table_name, (drop_cmd + create_cmd), drop_cmd
    else:
        print 'Writing to the original table in place. The location which was passed is being discarded.'
        repair_cmd = 'MSCK REPAIR TABLE %s;\n' % orig_table_name
        return orig_table_name, repair_cmd, ''


class TableProvided:
    def __init__(self, alias, table_name_resolver, path_param):
        self.table_alias = alias
        self.param = path_param

        if isfunction(table_name_resolver):
            self.table_name = table_name_resolver
        else:
            self.table_name = lambda **kwargs: table_name_resolver

    def assign_table_from_params(self, **kwargs):
        return temp_table_cmds(self.table_name(**kwargs), kwargs[self.param])

    def invoke_fnc(self, f, *args, **kwargs):
        effective_table_name, pre_cmd, post_cmd = self.assign_table_from_params(**kwargs)
        kwargs[self.table_alias] = effective_table_name
        return pre_cmd + f(*args, **kwargs) + post_cmd

    def __call__(self, fnc):
        return lambda *args, **kwargs: self.invoke_fnc(fnc, *args, **kwargs)


if __name__ == '__main__':
    print get_table_partitions('mobile.daily_app_metrics')
