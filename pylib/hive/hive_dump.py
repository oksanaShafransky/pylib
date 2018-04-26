import argparse

from  pylib.hive.table_utils import get_databases, get_tables, get_create_statement



def dump_database(db):

    dump_string  = "-- ------------------------------------------------------\n"
    dump_string += "-- Dumping tables for databse: %s\n" % db
    dump_string += "-- ------------------------------------------------------\n"
    dump_string += "\n"
    dump_string += "\n"
    dump_string += "CREATE DATABASE IF NOT EXISTS `%s`;\n" % db
    dump_string += "\n"

    for table_name in get_tables(db):
        dump_string += "-- Table structure for table : %s\n" % table_name
        dump_string += "\n"
        dump_string += "DROP TABLE IF EXISTS `%s.%s`;\n" % (db, table_name)
        dump_string += "\n"
        dump_string += get_create_statement(db, table_name)
        dump_string += "\n"
        dump_string += "MSCK REPAIR TABLE `%s.%s`;\n" % (db, table_name)
        dump_string += "\n"

    return dump_string



def dump_databases(db_list=None):
    db_list = db_list or get_databases()

    dump_string = ""

    for db in db_list:
        dump_string += dump_database(db)
        dump_string += "\n"

    return dump_string


if __name__ == '__main__':
    dump_databases()



