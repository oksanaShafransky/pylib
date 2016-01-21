__author__ = 'Felix'

from datetime import datetime
from datetime import timedelta

from airflow.plugins_manager import AirflowPlugin


def date_partition(date, in_date_fmt='%Y-%m-%d', **kwargs):
    date_parts = datetime.strftime(datetime.strptime(date, in_date_fmt), 'year=%y/month=%m/day=%d')
    other_partitions = ['%s=%s' % (key, value) for (key, value) in kwargs.iteritems()]
    return '/'.join([date_parts] + other_partitions)


def type_date_partition(date, mode_type, **kwargs):
    return 'type=%s/%s' % (mode_type, date_partition(date, **kwargs))


def hbase_table_suffix_partition(date, mode, mode_type, in_date_fmt='%Y-%m-%d'):
    date_fmt = '%_y_%m' if mode == 'snapshot' else '%_y_%m_%d'
    date_suffix = datetime.strftime(datetime.strptime(date, in_date_fmt), date_fmt)
    return date_suffix if mode == 'snapshot' else '_%s%s' % (mode_type, date_suffix)


def dss_in_same_month(ds1, ds2):
    ds1s = datetime.strptime(ds1, '%Y-%m-%d')
    ds2s = datetime.strptime(ds2, '%Y-%m-%d')
    return '%s' % str(ds1s.month == ds2s.month)


def last_day_of_month(date):
    if date.month == 12:
        return date.replace(day=31)
    return date.replace(month=date.month+1, day=1) - timedelta(days=1)


def last_interval_day(ds, interval):
    if interval == "@daily":
        return ds
    if interval == "@monthly":
        dsd = datetime.strptime(ds, '%Y-%m-%d')
        return last_day_of_month(dsd).isoformat()[:10]

class SWMacroAirflowPluginManager(AirflowPlugin):

    name = 'SWMacros'

    macros = [date_partition, type_date_partition, hbase_table_suffix_partition, dss_in_same_month, last_interval_day]

