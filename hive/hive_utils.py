__author__ = 'Felix'

import pyhs2
import getpass
from datetime import datetime


__author__ = 'Felix'

import pyhs2
import getpass
from datetime import datetime


class HiveHelper:
    def __init__(self, server='mrp-hive-a01'):
        self.conn = pyhs2.connect(server, authMechanism='PLAIN', user=getpass.getuser())

    def repair_table(self, table_name):
        with self.conn.cursor() as curr:
            curr.execute('msck repair table %s' % table_name)

    def get_table_partitions(self, table_name):
        with self.conn.cursor() as curr:
            curr.execute('show partitions %s' % table_name)
            return [dict([fld.split('=') for fld in part_def]) for part_def in [partition[0].split('/') for partition in curr.fetch()]]

    def get_table_dates(self, table_name):
        return [datetime.strptime('%02d-%02d-%02d' % (int(partition['year']) % 100, int(partition['month']), int(partition.get('day', 1))), '%y-%m-%d') for
                partition in self.get_table_partitions(table_name)
                ]

    def create_temp_table(self, original_table_name, cloned_table_name, location):
        with self.conn.cursor() as curr:
            curr.execute('create external table %s like %s location \'%s\'' % (cloned_table_name, original_table_name, location))

    def __del__(self):
        self.conn.close()
