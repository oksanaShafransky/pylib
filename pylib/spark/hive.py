from pylib.hive.hive_meta import get_table_location


def get_table_as_df(context, table, path, date=None, **additional_filters):
    """
    returns a hive table as df. able to deal with paths other than the original by creating a similar, temp table

    :param context: HiveContext to use
    :param table: hive table
    :param path: actual path
    :param date: date partition, used if not None
    :param additional_partitions: other filters to use, could be partitions or regular value filters
    :return: a df of either the original table, if path matches, or a similar temp table
    """

    if path == get_table_location(table):
        table_to_use = table
    else:
        pass