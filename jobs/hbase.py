from threading import Thread
import traceback

__author__ = 'Felix'

from random import random
from struct import *
import happybase
import sys
import cProfile, pstats
import cStringIO as StringIO


class Exporter:
    cf_params = {'max_versions': 1, 'compression': 'snappy'}

    batch = None
    MAX_BATCH_BYTES = 1024 * 1024 * 14
    HBASE_VERSION = '0.94'
    CONNECTION_REFRESH_INTERVAL = 20000

    def __init__(self, server_urls, table_name, col_family=None, col=None, create_table=False, overwrite=False,
                 batch_size=1):
        self.server_urls = server_urls
        self._conn = None
        self.count = 0
        self.bytes_in_batch = 0
        self.batch_size = batch_size
        self.table_name = table_name
        self.pr = cProfile.Profile()
        self.pr.enable()

        if create_table:
            table_exists = table_name in self.conn.tables()
            if table_exists:
                if overwrite:
                    self.conn.delete_table(table_name, disable=True)
                else:
                    raise 'Table already exists, use overwrite=True to force rewrite'

            self.conn.create_table(table_name, {col_family: self.cf_params})

        self._table = None
        self._batch = None
        self.col_family = col_family
        self.col = col

    @property
    def conn(self):
        if not self._conn:
            self.refresh_connection()
        return self._conn

    @property
    def batch(self):
        if not self._batch:
            self.refresh_connection()
        return self._batch

    @property
    def table(self):
        if not self._table:
            self.refresh_connection()
        return self._table


    def refresh_connection(self):
        if self._batch:
            self._batch.send()
        if self._conn:
            self._conn.close()
        for server_url in sorted(self.server_urls, key=lambda x: random()):
            try:
                self._conn = happybase.Connection(server_url, compat=self.HBASE_VERSION)
                sys.stderr.write('New thrift server url: %s\n' % server_url)
                break
            except:
                sys.stderr.write(traceback.format_exc())
                pass  # Should log this
        if not self._conn:
            raise EnvironmentError('No region server to connect to!')
        self._table = self._conn.table(self.table_name)
        self._batch = self.table.batch(batch_size=self.batch_size)

    @staticmethod
    def _run_in_thread(f, args, timeout=60 * 2):
        t = Thread(target=f, args=args)
        t.start()
        t.join(timeout)
        if t.is_alive():
            raise RuntimeError()


    def put(self, key, data):
        self.count += 1
        if self.count % self.batch_size == 0 and self.count > 0:
            sys.stderr.write('Wrote %s lines\n' % self.count)

        data_for_write = {}

        if self.col_family:
            if self.col:
                data_for_write = {('%s:%s' % (self.col_family, self.col)): data}
            else:
                for column in data:
                    data_for_write['%s:%s' % (self.col_family, column)] = data[column]
        else:
            data_for_write = data

        self.bytes_in_batch += len(key) + len(data_for_write)
        # try:
        # # self.batch.put(key, data_for_write)
        #     self._run_in_thread(self.batch.put, (key, data_for_write))
        # except RuntimeError:
        #     sys.stderr.write('Batch submit timed out\n')
        #     return
        self.batch.put(key, data_for_write)
        if self.bytes_in_batch > self.MAX_BATCH_BYTES:
            sys.stderr.write('Committing oversized (%s) batch...' % self.bytes_in_batch)
            self.bytes_in_batch = 0
            self.batch.send()
            sys.stderr.write('Done commiting.')
        if self.count % self.CONNECTION_REFRESH_INTERVAL == 0:
            self.refresh_connection()


    def _finish_profile(self):
        self.pr.disable()
        s = StringIO.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(self.pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        sys.stderr.write(s.getvalue())

    def __del__(self):
        if self.batch is not None:
            self.batch.send()
        self._conn.close()
        self._finish_profile()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.batch is not None:
            self.batch.send()
        self._conn.close()
        self._finish_profile()


class ByteHelper:
    def __init__(self):
        self._bytes = StringIO.StringIO()

    @property
    def bytes(self):
        return self._bytes.getvalue()

    def close(self):
        self._bytes.close()

    def __del__(self):
        self.close()

    def append_utf(self, bytes_or_unicode):
        if type(bytes_or_unicode) is unicode:
            encoded = str.encode('utf-8')
        else:
            encoded = bytes_or_unicode
        self.append_short(len(encoded))
        self._bytes.write(encoded)

    def append_short(self, num):
        self._bytes.write(pack('>h', num))

    def append_int(self, num):
        self._bytes.write(pack('>i', num))

    def append_double(self, num):
        self._bytes.write(pack('>d', num))

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
