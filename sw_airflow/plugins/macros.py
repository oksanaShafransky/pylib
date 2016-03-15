import calendar
from datetime import datetime
from datetime import timedelta

from airflow.plugins_manager import AirflowPlugin

__author__ = 'Felix'


def date_partition(ds, in_date_fmt='%Y-%m-%d', **kwargs):
    return generalized_date_partition(ds, 'daily', in_date_fmt, kwargs)


def generalized_date_partition(ds, mode, in_date_fmt='%Y-%m-%d', **kwargs):
    pattern = 'year=%y/month=%m' if mode == 'snapshot' else 'year=%y/month=%m/day=%d'
    date_parts = datetime.strftime(datetime.strptime(ds, in_date_fmt), pattern)
    other_partitions = ['%s=%s' % (key, value) for (key, value) in kwargs.iteritems()]
    return '/'.join([date_parts] + other_partitions)


def type_date_partition(ds, mode_type, **kwargs):
    return 'type=%s/%s' % (mode_type, date_partition(ds, **kwargs))


def hbase_table_suffix_partition(ds, mode, mode_type, in_date_fmt='%Y-%m-%d'):
    date_fmt = '_%y_%m' if mode == 'snapshot' else '_%y_%m_%d'
    date_suffix = datetime.strftime(datetime.strptime(ds, in_date_fmt), date_fmt)
    return date_suffix if mode == 'snapshot' else '_%s%s' % (mode_type, date_suffix)


def dss_in_same_month(ds1, ds2):
    ds1s = datetime.strptime(ds1, '%Y-%m-%d')
    ds2s = datetime.strptime(ds2, '%Y-%m-%d')
    return '%s' % str(ds1s.month == ds2s.month)


def last_day_of_month(ds, in_date_fmt='%Y-%m-%d'):
    dt = datetime.strptime(ds, in_date_fmt)
    days_in_month = calendar.monthrange(dt.year, dt.month)[1]
    return datetime(year=dt.year, month=dt.month, day=days_in_month)


def first_day_of_last_month(ds):
    if isinstance(ds, basestring):
        ds = datetime.strptime(ds, '%Y-%m-%d')
    ndt = ds.replace(day=1)
    ndt = ndt - timedelta(days=1)
    return datetime.strftime(ndt.replace(day=1), '%Y-%m-%d')


def last_interval_day(ds, interval):
    if interval == '@daily':
        return ds
    if interval == '@monthly':
        dsd = datetime.strptime(ds, '%Y-%m-%d')
        return last_day_of_month(dsd).isoformat()[:10]


class SWMacroAirflowPluginManager(AirflowPlugin):
    name = 'SWMacros'

    macros = [date_partition, generalized_date_partition, type_date_partition, hbase_table_suffix_partition,
              dss_in_same_month, last_interval_day, first_day_of_last_month]
