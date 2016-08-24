import re

__author__ = 'Felix'

from datetime import datetime

from pylib.hive import common
from pylib.hive.common import formatted
from pylib.hive.table_utils import *


@formatted
@TableProvided(alias='target_table', table_name_resolver='felix.sample_app_metrics', path_param='output_table_path')
@TableProvided(alias='metrics_table', table_name_resolver='mobile.daily_app_metrics', path_param='metrics_table_path')
def sample_user_data(date, metrics_table_path, output_table_path, limit=10, **kwargs):
    year, month, day = common.parse_date(date)
    partition_str = common.getDatePartitionString(year, month, day)
    where_str = common.get_monthly_where(year, month, day)

    hql = '''
              insert overwrite table %(target_table)s partition %(partition)s
              select app, country, devices
              from %(source_table)s
              where %(clause)s
              ;
          ''' \
          % \
          {
              'partition': partition_str,
              'source_table': kwargs['metrics_table'],
              'target_table': kwargs['target_table'],
              'clause': where_str
          }

    return hql


def test_simple_query():
    expected_query = '''drop table if exists felix.sample_app_metrics;
create external table felix.sample_app_metrics
like felix.sample_app_metrics
location '/home/felix/temp-tables2/app-metrics';
use felix;
msck repair table sample_app_metrics;
drop table if exists mobile.daily_app_metrics;
create external table mobile.daily_app_metrics
like mobile.daily_app_metrics
location '/similargroup/data/mobile-analytics/daily/aggregate/aggkey=appcountrysourcekey';
use mobile;
msck repair table daily_app_metrics;
insert overwrite table felix.sample_app_metrics partition (year=16, month=02, day=15)
select app, country, devices
from mobile.daily_app_metrics
where year=16 and month=02 and day=15
;
drop table if exists mobile.daily_app_metrics;
drop table if exists felix.sample_app_metrics;'''

    actual_query = sample_user_data(datetime(2016, 2, 15),
                                    metrics_table_path='/similargroup/data/mobile-analytics/daily/aggregate/aggkey=AppCountrySourceKey',
                                    output_table_path='/home/felix/temp-tables2/app-metrics')
    processed_actual_query = re.sub(pattern='_temp_\d*', repl='', string=actual_query).strip().lower()
    assert processed_actual_query == expected_query
