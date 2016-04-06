__author__ = 'Felix'

import consulate

from pylib.sw.config.kv import KeyValueProxy


class ConsulProxy(KeyValueProxy):
    def __init__(self, service):
        self.client = consulate.Consul(service)

    def get(self, key):
        return self.client.kv.get(str(key))

    def set(self, key, value):
        self.client.kv.set(str(key), str(value))

    def delete(self, key):
        self.client.kv.delete(str(key), recurse=True)

    def sub_keys(self, key):
        return [sub_key for sub_key in [sub_key[len(str(key)) + 1:] for
                sub_key in self.client.kv.find('str(key)')] if '/' not in sub_key]