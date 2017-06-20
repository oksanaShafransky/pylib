from datetime import timedelta
from pylib.hive import common
from pylib.tasks.ptask_infra import *
from pylib.hadoop.hdfs_util import copy_dir_from_path


class DataCompleter(object):

    """
    This is a utility for completing data collections from past dates

    Usage Example:

     table_paths = {'daily_country_source_device_metrics': '/similargroup/data/mobile-analytics/daily/aggregate/aggkey=CountrySourceDeviceKey',
                   'daily_app_metrics': '/imilargroup/data/mobile-analytics/daily/aggregate/aggkey=ApspCountrySourceKey'}

    data_completions = [{'date_to_complete': '2017-03-20',
                     'days_delta' : 7,
                     'type' : 'hive',
                     'tables': ['daily_country_source_device_metrics','daily_app_metrics'],
                     'conditions' : {'complete': 'source in (650, 653)',
                                     'keep': 'source not in (650, 653)'},
                     'description': 'An array of sources sent partial data in that day, so their data is invalid'}]


    @ptask
    def fix_daily_aggregation(ctx):
        ti = ContextualizedTasksInfra(ctx)
        DataCompleter(ti).complete_data(data_completions,
                                        table_paths)
    """



    def __init__(self, contextualized_tasks_infra):
        self.ti = contextualized_tasks_infra

    def __complete_data_hive_query(self,
                       table_full_name,
                       src_where,
                       dst_where,
                       dst_partition,
                       conditions):
        if conditions != {}:
            where_str = "(%s AND %s) OR (%s AND %s)" % (dst_where,
                                                        conditions['keep'],
                                                        src_where,
                                                        conditions['complete'])
        else:
            where_str = src_where
        query = '''INSERT OVERWRITE TABLE %(table_full_name)s PARTITION%(dst_partition)s
                   SELECT `(year|month|day)?+.+` FROM %(table_full_name)s WHERE %(where_str)s;
                ''' % \
                {'table_full_name': table_full_name,
                 'src_where': src_where,
                 'dst_partition': dst_partition,
                 'where_str': where_str
                 }
        return query

    def __complete_data_hive_collection(self, db, table_path, day_delta, conditions, table_name):
        table_full_name = ".".join([db, table_name])
        dst_year, dst_month, dst_day = common.parse_date(self.ti.date)
        src_year, src_month, src_day = common.parse_date(self.ti.date - timedelta(days=day_delta))
        dst_partition = common.getDatePartitionString(dst_year, dst_month, dst_day)
        src_where = common.get_monthly_where(src_year, src_month, src_day)
        dst_where = common.get_monthly_where(dst_year, dst_month, dst_day)
        hive_settings = '''SET hive.support.quoted.identifiers=none;\n'''
        table_full_name, table_drop_cmd, table_create_cmd = common.temp_table_cmds(table_full_name, table_path)
        query = self.__complete_data_hive_query(table_full_name=table_full_name,
                               src_where=src_where, dst_where=dst_where,
                               dst_partition=dst_partition, conditions=conditions)
        print ("Date in hive holes list, Moving from: %s to: %s" % (src_where, dst_where))
        self.ti.run_hive(hive_settings + table_drop_cmd + table_create_cmd + query,
                    query_name="Overwrite data with previous date according to some condition")

    def __complete_data_hdfs_collection(ti, hdfs_path, day_delta):
        source = "/".join([hdfs_path, TasksInfra.year_month_day(ti.date - timedelta(days=day_delta))])
        target = "/".join([hdfs_path, TasksInfra.year_month_day(ti.date)])
        print ("Date in HDFS holes list, Moving from: %s to: %s" % (source, target))
        if not ti.dry_run:
            copy_dir_from_path(source, target)

    def complete_data(self, completions, table_paths=[]):
        date_str = self.ti.date.isoformat()
        for completion in completions:
            if date_str == completion['date_to_complete']:
                print ('Applying data-complete - %s' % completion['description'])
                if completion['type'] == 'hive':
                    for table in completion['tables']:
                        table_path = table_paths[table]
                        print ('Applying the data-complete for hive table %s whose path is %s' % (table, table_path))
                        self.__complete_data_hive_collection('mobile', table_path, completion['days_delta'], completion['conditions'], table)
                if completion['type'] == 'hdfs':
                    for hdfs_path in completion['paths']:
                        print ('Applying the data-complete for hive table %s whose path is %s' % (table, table_path))
                        self.__complete_data_hdfs_collection(hdfs_path, completion['days_delta'])
