from pylib.hive.hive_meta import get_table_location


def date_partition(dt):
    return {'year': dt.strftime('%y'), 'month': dt.strftime('%m'), 'day': dt.strftime('%d')}


def get_table_as_df(context, table, path=None, dt=None, **additional_filters):
    """
    returns a hive table as df. able to deal with paths other than the original by creating a similar, temp table

    :param context: HiveContext to use
    :param table: hive table
    :param path: actual path
    :param dt: date partition, used if not None
    :param additional_filters: other filters to use, could be partitions or regular value filters
    :return: a df of either the original table, if path matches, or a similar temp table
    """

    if path is None or path == get_table_location(table):
        context.sql('MSCK REPAIR TABLE %s' % table)
        table_to_use = context.table(table)
    else:
        schema = context.table(table).schema
        from random import randrange
        temp_table_name = '%s_tmp_%d' % (table, randrange(1000))
        table_to_use = context.createExternalTable(temp_table_name, path, schema=schema)
        context.sql('MSCK REPAIR TABLE %s' % temp_table_name)
        context.sql('DROP TABLE %s' % temp_table_name)

    filters = date_partition(dt) if dt is not None else dict()
    filters.update(additional_filters)

    ret = table_to_use

    # tolerate filters for non existing columns by ignoring them
    for filtered_col, filter_value in filters.items():
        if filtered_col in ret.columns:
            ret = ret.filter(ret[filtered_col] == filter_value)

    return ret
