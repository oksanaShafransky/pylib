from datetime import timedelta
from pylib.hive import common
from pylib.tasks.ptask_infra import *
from pylib.hadoop.hdfs_util import copy_dir_from_path


class DataCompleter(object):

    """
    This is a utility for completing data collections from past dates

    Usage Example:

    table_paths = {'daily_country_source_device_metrics': '/similargroup/data/mobile-analytics/daily/aggregate/aggkey=CountrySourceDeviceKey',
                   'daily_app_metrics': '/similargroup/data/mobile-analytics/daily/aggregate/aggkey=AppCountrySourceKey'}

    data_completions = [
        HiveDataCompletion(date_to_complete='2017-03-20',
                           description='An array of sources sent partial data in that day, so their data is invalid',
                           db='mobile',
                           table_paths=table_paths,
                           day_delta= 7,
                           conditions= {'complete': 'source in (650, 653)',
                                        'keep': 'source not in (650, 653)'},
                           table_names=['daily_country_source_device_metrics','daily_app_metrics'],
                           )
        ]


    @ptask
    def fix_daily_aggregation(ctx):
        ti = ContextualizedTasksInfra(ctx)
        DataCompleter(ti).complete_data(data_completions,
                                    table_paths)

    """



    def __init__(self, contextualized_tasks_infra):
        self.ti = contextualized_tasks_infra

    def complete_data(self, completers):
        date_str = self.ti.date.isoformat()
        for completer in completers:
            if date_str == completer.date_to_complete:
                print ('Applying data-complete - %s' % completer.description)
                completer.complete(self.ti)


class AbstractDataCompletion(object):
    def __init__(self, date_to_complete, description):
        self.date_to_complete = date_to_complete
        self.description = description

    def complete(self, contextualizedTaskInfra):
        print ('Applying data-complete - %s' % self.description)
        self.do_complete(contextualizedTaskInfra)

    def do_complete(self, contextualizedTaskInfra):
        raise NotImplementedError


class HiveDataCompletion(AbstractDataCompletion):
    def __init__(self, db, table_paths, day_delta, conditions, table_names, *args, **kwargs):
        super(HiveDataCompletion, self).__init__(*args, **kwargs)
        self.db = db
        self.table_paths = table_paths
        self.day_delta = day_delta
        self.conditions = conditions
        self.table_names = table_names

    @staticmethod
    def __complete_data_hive_query(table_full_name,
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

    def do_complete(self, ti):
        db = 'mobile'
        for table_name in self.table_names:
            table_path = self.table_paths[table_name]
            print ('Applying the data-complete for hive table %s whose path is %s' % (table_name, table_path))
            table_full_name = ".".join([db, table_name])
            dst_year, dst_month, dst_day = common.parse_date(ti.date)
            src_year, src_month, src_day = common.parse_date(ti.date - timedelta(days=self.day_delta))
            dst_partition = common.getDatePartitionString(dst_year, dst_month, dst_day)
            src_where = common.get_monthly_where(src_year, src_month, src_day)
            dst_where = common.get_monthly_where(dst_year, dst_month, dst_day)
            hive_settings = '''SET hive.support.quoted.identifiers=none;\n'''
            table_full_name, table_drop_cmd, table_create_cmd = common.temp_table_cmds(table_full_name, table_path)
            query = self.__complete_data_hive_query(table_full_name=table_full_name,
                                                    src_where=src_where, dst_where=dst_where,
                                                    dst_partition=dst_partition, conditions=self.conditions)
            print ("Date in hive holes list, Moving from: %s to: %s" % (src_where, dst_where))
            ti.run_hive(hive_settings + table_drop_cmd + table_create_cmd + query,
                             query_name="Overwrite data with previous date according to some condition")


class HdfsDataCompletion(AbstractDataCompletion):
    def __init__(self, day_delta, hdfs_path, *args, **kwargs):
        super(HiveDataCompletion, self).__init__(*args, **kwargs)
        self.hdfs_path = hdfs_path
        self.day_delta = day_delta

    def do_complete(self, ti):
        source = "/".join([self.hdfs_path, TasksInfra.year_month_day(ti.date - timedelta(days=self.day_delta))])
        target = "/".join([self.hdfs_path, TasksInfra.year_month_day(ti.date)])
        print ("Date in HDFS holes list, Moving from: %s to: %s" % (source, target))
        if not ti.dry_run:
            copy_dir_from_path(source, target)
