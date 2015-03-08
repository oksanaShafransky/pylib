__author__ = 'Felix'

from struct import *
import happybase

class Exporter:

    cf_params = {'max_versions': 1, 'compression': 'snappy'}

    batch = None

    def __init__(self, db_url, table_name, col_family=None, col=None, create_table=False, overwrite=False, batch_size=1):
        self.conn = happybase.Connection(db_url)

        if create_table:
            table_exists = table_name in self.conn.tables()
            if table_exists:
                if overwrite:
                    self.conn.delete_table(table_name, disable=True)
                else:
                    raise 'Table already exists, use overwrite=True to force rewrite'

            self.conn.create_table(table_name, {col_family: self.cf_params})

        self.table = self.conn.table(table_name)
        self.batch = self.table.batch(batch_size=batch_size)
        self.col_family = col_family
        self.col = col

    def put(self, key, data):
        data_for_write = {}

        if self.col_family:
            if self.col:
                data_for_write = {('%s:%s' % (self.col_family, self.col)): data}
            else:
                for column in data:
                    data_for_write['%s:%s' % (self.col_family, column)] = data[column]
        else:
            data_for_write = data

        self.batch.put(key, data_for_write)

    def __del__(self):
        if self.batch is not None:
            self.batch.send()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.batch is not None:
            self.batch.send()


class ByteHelper:

    def __init__(self):
        self._bytes = ''

    @property
    def bytes(self):
        return self._bytes

    def append_utf(self, str):
        encoded = str.encode('utf-8')
        self.append_short(len(encoded))
        self._bytes += encoded

    def append_short(self, num):
        self._bytes += pack('>h', num)

    def append_int(self, num):
        self._bytes += pack('>i', num)

    def append_double(self, num):
        self._bytes += pack('>d', num)

    def append_collection(self, collection, types):
        self.append_int(len(collection))
        for record in collection:
            for idx in range(len(types)):
                val = record[idx]
                if types[idx] == 'str':
                    self.append_utf(val)
                elif types[idx] == 'int':
                    self.append_int(val)
                elif types[idx] == 'double':
                    self.append_double(val)
                else:
                    raise 'Unknown Type'
