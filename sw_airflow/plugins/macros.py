__author__ = 'Felix'

from datetime import datetime

from airflow.plugins_manager import AirflowPlugin


def date_partition(date, in_date_fmt='%Y-%m-%d', **kwargs):
    date_parts = datetime.strftime(datetime.strptime(date, in_date_fmt), 'year=%y/month=%m/day=%d')
    other_partitions = ['%s=%s' % (key, value) for (key, value) in kwargs.iteritems()]
    return '/'.join([date_parts] + other_partitions)


def type_date_partition(date, mode_type, **kwargs):
    return 'type=%s/%s' % (mode_type, date_partition(date, **kwargs))


def dss_in_same_month(ds1, ds2):
    ds1s = datetime.strptime(ds1, '%Y-%m-%d')
    ds2s = datetime.strptime(ds2, '%Y-%m-%d')
    return '%s' % str(ds1s.month == ds2s.month)

class SWMacroAirflowPluginManager(AirflowPlugin):

    name = 'SWMacros'

    macros = [date_partition, type_date_partition, dss_in_same_month]

