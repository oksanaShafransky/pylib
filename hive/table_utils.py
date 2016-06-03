from common import temp_table_cmds, temp_hbase_table_cmds

try:
    import pyhs2
except ImportError as e:
    pass
import getpass
from datetime import datetime
from dateutil import parser
from inspect import isfunction

__author__ = 'Felix'


def trim(s):
    return s.strip(' \t\r\n')


HIVE_SERVER = 'hive-server2-mrp.service.production'


def get_hive_connection():
    return pyhs2.connect(HIVE_SERVER, authMechanism='PLAIN', user=getpass.getuser())


def get_databases():
    with get_hive_connection().cursor() as curr:
        curr.execute('show databases')
        return [item[0] for item in curr.fetch()]


def get_tables(db):
    with get_hive_connection().cursor() as curr:
        curr.execute('use %s' % db)
        curr.execute('show tables')
        return [item[0] for item in curr.fetch()]


def repair_table(table_name):
    with get_hive_connection().cursor() as curr:
        curr.execute('msck repair table %s' % table_name)


def get_table_partitions(table_name):
    with get_hive_connection().cursor() as curr:
        curr.execute('show partitions %s' % table_name)
        return [dict([fld.split('=') for fld in part_def]) for part_def in
                [partition[0].split('/') for partition in curr.fetch()]]


def get_table_dates(table_name):
    return [datetime.strptime(
        '%02d-%02d-%02d' % (int(partition['year']) % 100, int(partition['month']), int(partition.get('day', 1))),
        '%y-%m-%d') for
            partition in get_table_partitions(table_name)
            ]


def create_temp_table(original_table_name, cloned_table_name, location):
    with get_hive_connection().cursor() as curr:
        curr.execute(
            'create external table %s like %s location \'%s\'' % (cloned_table_name, original_table_name, location))


def get_table_info(table_name):
    with get_hive_connection().cursor() as curr:
        curr.execute('desc formatted %s' % table_name)
        return list(curr.fetch())


def is_external_table(table_name):
    is_ext_info = [line for line in get_table_info(table_name) if trim(line[0]) == 'Table Type:'][0]
    return trim(is_ext_info[1]).lower() == 'external_table'


def get_table_create_time(table_name):
    create_time_info = [line for line in get_table_info(table_name) if trim(line[0]) == 'CreateTime:'][0]
    return parser.parse(create_time_info[1])


def delete_table(table_name):
    with get_hive_connection().cursor() as curr:
        curr.execute('drop table %s' % table_name)


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
        effective_table_name, drop_cmd, create_cmd = self.assign_table_from_params(**kwargs)
        pre_cmd = drop_cmd + create_cmd
        post_cmd = drop_cmd
        kwargs[self.table_alias] = effective_table_name
        return pre_cmd + f(*args, **kwargs) + post_cmd

    def __call__(self, fnc):
        return lambda *args, **kwargs: self.invoke_fnc(fnc, *args, **kwargs)


class HBaseTableProvided:
    def __init__(self, alias, table_name_resolver, mode_param='mode', mode_type_param='mode_type', date_param='date'):
        self.table_alias = alias
        self.mode_param, self.mode_type_param, self.date_param = mode_param, mode_type_param, date_param

        if isfunction(table_name_resolver):
            self.table_name = table_name_resolver
        else:
            self.table_name = lambda **kwargs: table_name_resolver

    def assign_table_from_params(self, **kwargs):
        return temp_hbase_table_cmds(self.table_name(**kwargs), kwargs[self.mode_param], kwargs[self.mode_type_param], kwargs[self.date_param])

    def invoke_fnc(self, f, *args, **kwargs):
        effective_table_name, drop_cmd, create_cmd = self.assign_table_from_params(**kwargs)
        pre_cmd = drop_cmd + create_cmd
        post_cmd = drop_cmd
        kwargs[self.table_alias] = effective_table_name
        return pre_cmd + f(*args, **kwargs) + post_cmd

    def __call__(self, fnc):
        return lambda *args, **kwargs: self.invoke_fnc(fnc, *args, **kwargs)


if __name__ == '__main__':
    print get_table_partitions('mobile.daily_app_metrics')
