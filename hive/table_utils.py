__author__ = 'Felix'

import os
import sys
import subprocess
import random
import logging
from urlparse import urlparse
from inspect import isfunction


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
    logger.info('Checking whether to create external table %s in location %s:' % (orig_table_name, root))
    if root is not None and should_create_external_table(orig_table_name, root):
        logger.info('Writing to an external table in the given location.')
        table_name, drop_cmd, create_cmd = temp_table_cmds_internal(orig_table_name, root)
        return table_name, (drop_cmd + create_cmd), drop_cmd
    else:
        logger.info('Writing to the original table in place. The location which was passed is being discarded.')
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
