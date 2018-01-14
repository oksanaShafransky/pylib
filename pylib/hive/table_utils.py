from common import temp_table_cmds, temp_hbase_table_cmds

try:
    import pyhs2
except ImportError as e:
    pass
import getpass
from datetime import datetime, timedelta
from dateutil import parser, relativedelta
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
        '%02d-%02d-%02d' % (int(partition['year']) % 100,
                            int(partition['month']),
                            int(partition.get('day', 1))),
        '%y-%m-%d')
        for partition in get_table_partitions(table_name)
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


def get_table_partition_info(table_name, partition):
    partition_str = ', '.join(["%s='%s'" % (k, v) for k, v in partition.items()])

    with get_hive_connection().cursor() as curr:
        curr.execute('describe formatted %s partition (%s)' % (table_name, partition_str))
        return list(curr.fetch())


def drop_partition(table_name, partition):
    partition_str = ', '.join(["%s='%s'" % (k, v) for k, v in partition.items()])
    with get_hive_connection().cursor() as curr:
        curr.execute('alter table %s drop if exists partition (%s)' % (table_name, partition_str))


def drop_partition_str(table_name, partition_str):
    with get_hive_connection().cursor() as curr:
        curr.execute('alter table %s drop if exists partition %s' % (table_name, partition_str))


def _get_table_partition_path(table_name, partition):
    partition_info = get_table_partition_info(table_name, partition)
    for info in partition_info:
        if str.startswith(info[0], 'Location'):
            path =  info[1]
            # remove namenode hostname for hdfs locations
            if 'hdfs://' in path:
                return path[path.find('/', start=7)]
            else:
                return path


def _part_str(table_mode, year, month, day=None):
    year_str = str(year)
    if len(year_str) > 2:
        year_str = year_str[2:]

    month_str = str(month).zfill(2)

    if day:
        day_str = str(day).zfill(2)
    else:
        day_str = None

    if table_mode == 'daily':
        partition_pattern = "{'month':'%s','day':'%s','year':'%s'}" % (month_str, day_str, year_str)
    elif table_mode == 'window':
        partition_pattern = "{'month':'%s','day': '%s','year': '%s','mode_type':'last-28'}" % (month_str, day_str, year_str)
    elif table_mode == 'snapshot':
        partition_pattern = "{'month': '%s','year': '%s','mode_type':'monthly'}" % (month_str, year_str)
    else:
        raise ValueError('Unable to determine mode_type')
    return partition_pattern


def _get_lookback(rundate, lookback, lookback_interval):
    if lookback_interval == 'daily':
        date_range = [rundate - timedelta(days=x) for x in range(0, lookback)]
    elif lookback_interval == 'monthly':
        date_range = [rundate - relativedelta.relativedelta(months=x) for x in range(0, lookback)]
    else:
        raise ValueError('{} invalid lookback interval'.format(lookback_interval))
    return date_range


def get_table_partition_paths(rundate, table_name, mode, lookback, lookback_interval='daily'):
    '''
    Return a list of paths for a hive table's partitions.

    :param rundate: the job's run date
    :type rundate: datetime
    :param table_name: name of hive table, including db
    :type table_name: str
    :param mode: hive table's mode, used to determine partition pattern. Can be daily, window or snapshot
    :type mode: str
    :param lookback: number of days back to search for partitions from and including the run date,
    :type lookback: int
    :param lookback_interval: the interval for finding partitions, daily or monthly
    :param lookback_interval: str
    :return: list of paths
    '''

    date_range = _get_lookback(rundate, lookback, lookback_interval)
    parts = get_table_partitions(table_name)

    # does the table mode include days
    days = False if mode == 'snapshot' else True

    part_strings = {_part_str(mode, s['year'], s['month'], s['day'] if days else None): s for s in parts}

    missing_partitions = []
    partition_paths = []
    missing_partition_paths = []

    for day in date_range:
        # check if we have a partition corresponding to the day
        day_str = _part_str(mode, day.year, day.month, day.day if days else None)
        if part_strings.get(day_str) is None:
            missing_partitions.append(day)
            continue

        # get the path for each partition
        partition_path = _get_table_partition_path(table_name, part_strings[day_str])
        if partition_path is None:
            missing_partition_paths.append(day_str)

        partition_paths.append(partition_path)

    assert len(missing_partitions) == 0, \
        "The following partitions were not found in the hive metastore: %s" % str(missing_partitions)
    assert len(partition_paths) == lookback, \
        "Could not find paths for the following partitions: %s" % str(missing_partition_paths)

    return partition_paths


class TableProvided(object):
    def __init__(self, alias, table_name_resolver, path_param, pre_post=True):
        self.table_alias = alias
        self.param = path_param
        self.pre_post = pre_post

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
        if self.pre_post:
            return pre_cmd + f(*args, **kwargs) + post_cmd
        else:
            return f(*args, **kwargs)

    def __call__(self, fnc):
        return lambda *args, **kwargs: self.invoke_fnc(fnc, *args, **kwargs)


class HBaseTableProvided(object):
    def __init__(self, alias, table_name_resolver, mode_param='mode', mode_type_param='mode_type', date_param='date',
                 hbase_table_name_param='hbase_table_name'):
        self.hbase_table_name = hbase_table_name_param
        self.table_alias = alias
        self.mode_param, self.mode_type_param, self.date_param = mode_param, mode_type_param, date_param

        if isfunction(table_name_resolver):
            self.table_name = table_name_resolver
        else:
            self.table_name = lambda **kwargs: table_name_resolver

    def assign_table_from_params(self, **kwargs):
        return temp_hbase_table_cmds(self.table_name(**kwargs), kwargs[self.hbase_table_name], kwargs[self.mode_param],
                                     kwargs[self.mode_type_param], kwargs[self.date_param])

    def invoke_fnc(self, f, *args, **kwargs):
        effective_table_name, drop_cmd, create_cmd = self.assign_table_from_params(**kwargs)
        pre_cmd = drop_cmd + create_cmd
        post_cmd = drop_cmd
        kwargs[self.table_alias] = effective_table_name
        return pre_cmd + f(*args, **kwargs) + post_cmd

    def __call__(self, fnc):
        return lambda *args, **kwargs: self.invoke_fnc(fnc, *args, **kwargs)


if __name__ == '__main__':
    print(get_table_partitions('mobile.daily_app_metrics'))
