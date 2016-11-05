__author__ = 'Felix'

import etcd

from kv import KeyValueProxy


class EtcdProxy(KeyValueProxy):
    def __init__(self, server, port=4001, root_path='v1'):
        super(EtcdProxy, self).__init__()
        self.client = etcd.Client(server, port)
        self.root_path = '/' + root_path if not root_path.startswith('/') else root_path

    def _full_path(self, path):
        return '%s/%s' % (self.root_path, path)

    def get(self, key):
        return str(self.client.get(self._full_path(str(key))).value)

    def set(self, key, value):
        return self.client.set(self._full_path(str(key)), str(value))

    def delete(self, key):
        return self.client.delete(self._full_path(str(key)))

    def sub_keys(self, key):
        return [res.key[len(self._full_path(str(key))) + 1:] for res in self.client.get(self._full_path(str(key))).children]

