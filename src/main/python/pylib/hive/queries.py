__author__ = 'Felix'

from pylib.hive.common import *
from pylib.hive.table_utils import TableProvided


@formatted
@TableProvided(alias='source_table', table_name_resolver=lambda **kwargs: kwargs['daily_table'], path_param='daily_table_path')
@TableProvided(alias='target_table', table_name_resolver=lambda **kwargs: kwargs['sum_table'], path_param='sum_table_path')
def sum_query(dt, mode, mode_type, daily_table, daily_table_path, sum_table, sum_table_path, group_cols, summed_cols, filters=None, **kwargs):
    year, month, day = parse_date(dt)
    partition_str = getPartitionString(mode, mode_type, year, month, day)

    return """
                                      use %(db)s;
                                      insert overwrite table %(sum_table)s partition %(partition_str)s
                                      select %(key_cols)s, %(summed_cols)s
                                      from %(daily_table)s a
                                      where %(where_clause)s
                                      group by %(group_cols)s;
                                    """ \
    % \
                                    {
                                        'sum_table': kwargs['target_table'],
                                        'daily_table': kwargs['daily_table'],
                                        'partition_str': partition_str,
                                        'key_cols': ','.join(group_cols),
                                        'summed_cols': ','.join(['sum(%s)' % col for col in summed_cols]),
                                        'where_clause': ' AND '.join(
                                            [get_range_where_clause(year, month, day, mode, mode_type)] +
                                            (filters or [])
                                        )
                                    }
