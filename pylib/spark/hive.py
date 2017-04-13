from pylib.hive.hive_meta import get_table_location


def date_partition(dt):
    return {'year': dt.strftime('%y'), 'month': dt.strftime('%m'), 'day': dt.strftime('%d')}


class HiveHelper(object):
    def __init__(self, sql_context):
        self._ctx = sql_context
        self._temp_tables = []

    def get_table_as_df(self, table, path=None, dt=None, **additional_filters):
        """
        returns a hive table as df. able to deal with paths other than the original by creating a similar, temp table
        temp tables created can be cleaned by calling drop_temp_tables method of this HiveHelper
        it is also automatically called within __exit__ to use with

        :param table: hive table
        :param path: actual path
        :param dt: date partition, used if not None
        :param additional_filters: other filters to use, could be partitions or regular value filters
        :return: a df of either the original table, if path matches, or a similar temp table
        """

        if path is None or path == get_table_location(table):
            table_to_use = table
        else:
            from random import randrange
            table_to_use = '%s_tmp_%d' % (table, randrange(1000))
            self._ctx.sql('''CREATE EXTERNAL TABLE %s LIKE %s LOCATION '%s' ''' % (table_to_use, table, path))
            self._temp_tables.append(table_to_use)

        self._ctx.sql('MSCK REPAIR TABLE %s' % table_to_use)

        filters = date_partition(dt) if dt is not None else dict()
        filters.update(additional_filters)

        ret = self._ctx.table(table_to_use)

        # tolerate filters for non existing columns by ignoring them
        for filtered_col, filter_value in filters.items():
            if filtered_col in ret.columns:
                ret = ret.filter(ret[filtered_col] == filter_value)

        return ret

    def drop_temp_tables(self):
        for tmp_table in self._temp_tables:
            self._ctx.sql('DROP TABLE %s' % tmp_table)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.drop_temp_tables()

