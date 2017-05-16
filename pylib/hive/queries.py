__author__ = 'Felix'

from pylib.hive.common import *
from pylib.hive.table_utils import TableProvided, HBaseTableProvided


@formatted
@TableProvided(alias='source_table', table_name_resolver=lambda **kwargs: kwargs['daily_table'], path_param='daily_table_path')
@TableProvided(alias='target_table', table_name_resolver=lambda **kwargs: kwargs['sum_table'], path_param='sum_table_path')
def sum_query(dt, mode, mode_type, daily_table, daily_table_path, sum_table, sum_table_path, group_cols, summed_cols, filters=None, input_range_mode_type=None, **kwargs):
    year, month, day = parse_date(dt)
    partition_str = getPartitionString(mode, mode_type, year, month, day)

    return """
                                      insert overwrite table %(sum_table)s partition %(partition_str)s
                                      select %(key_cols)s, %(summed_cols)s
                                      from %(daily_table)s a
                                      where %(where_clause)s
                                      group by %(key_cols)s;
                                    """ \
    % \
                                    {
                                        'sum_table': kwargs['target_table'],
                                        'daily_table': kwargs['source_table'],
                                        'partition_str': partition_str,
                                        'key_cols': ','.join(group_cols),
                                        'summed_cols': ','.join(['sum(%s)' % col for col in summed_cols]),
                                        'where_clause': ' AND '.join(
                                            [get_range_where_clause(year, month, day, mode, input_range_mode_type or mode_type)] +
                                            (filters or [])
                                        )
                                    }


@formatted
@deploy_jars
@TableProvided(alias='source_table', table_name_resolver=lambda **kwargs: '%s.%s' % (kwargs['hive_db'],kwargs['source_table_name']), path_param='source_table_path')
@HBaseTableProvided(alias='target_hbase_table', table_name_resolver=lambda **kwargs: '%s.%s' % (kwargs['hive_db'], kwargs['target_hive_template']), hbase_table_name_param='hbase_table_name')
def load_to_hbase_query(date, mode, mode_type, hive_db, source_table_name, source_table_path, target_hive_template, hbase_table_name, deploy_path, cols, where_filters=None, temp_functions=None, input_range_mode_type=None, **kwargs):
    year, month, day = parse_date(date)
    return '''
        use %(hive_db)s;
        %(create_functions)s
        INSERT OVERWRITE TABLE %(target_hbase_table)s
        SELECT %(cols)s
        FROM %(source_table)s
        where %(where_clause)s;
        ''' \
           % \
           {
               'hive_db': hive_db,
               'create_functions': '\n'.join(["CREATE TEMPORARY FUNCTION %s AS '%s';" % (name, as_class)
                                              for (name, as_class) in temp_functions]),
               'cols': ','.join(cols),
               'target_hbase_table': kwargs['target_hbase_table'],
               'source_table': kwargs['source_table'],
               'where_clause': ' AND '.join(
                   [get_range_where_clause(year, month, day, mode, input_range_mode_type or mode_type)] +
                   (where_filters or []))
           }


@formatted
@TableProvided(alias='scraped_kw_hdfs', table_name_resolver=lambda **kwargs: '%s.%s_kw_scraping' % (kwargs['hive_db'], kwargs['mode']), path_param='output_path')
@HBaseTableProvided(alias='scraped_kw_hbase', table_name_resolver=lambda **kwargs: '%s.%s_%s' % (kwargs['hive_db'], kwargs['mode'], kwargs['scraping_hbase_table_name']))
def extract_scraped_keywords(date, mode, mode_type, hive_db, hbase_table_name, scraping_hbase_table_name, output_path, **kwargs):
    year, month, day = parse_date(date)
    partition_str = getDatePartitionString(year, month)

    query = """
            use %(db)s;
            insert overwrite table %(target_table)s partition %(partition_str)s
            SELECT
                sr_keyword,
                map_keys(data) as ts,
                map_values(data) as page_data
            FROM %(scraped_hbase)s;
        """ % {
        'db': hive_db,
        'target_table': kwargs['scraped_kw_hdfs'],
        'partition_str': partition_str,
        'scraped_hbase': kwargs['scraped_kw_hbase']
    }

    return query
