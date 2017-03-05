__author__ = 'Felix'

import consulate

from kv import KeyValueProxy


class ConsulProxy(KeyValueProxy):
    def __init__(self, server, token=None):
        super(ConsulProxy, self).__init__()
        self.client = consulate.Consul(server, token=token)

    def get(self, key):
        return self.client.kv.get(str(key))

    def set(self, key, value):
        return self.client.kv.set(str(key), str(value))

    def delete(self, key):
        return self.client.kv.delete(str(key), recurse=True)

    def sub_keys(self, key):
        return [sub_key for sub_key in [sub_key[len(str(key)) + 1:] for
                                        sub_key in self.client.kv.find(str(key))] if '/' not in sub_key]

    def __str__(self):
        return 'consul key value server=%s' % self.client.status.leader()
