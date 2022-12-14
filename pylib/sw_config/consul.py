from kv import KeyValueProxy

import logging
import consulate
# Adjust log level
logging.getLogger('consulate').setLevel(logging.WARNING)


class ConsulProxy(KeyValueProxy):
    def __init__(self, server, token=None, dc=None):
        super(ConsulProxy, self).__init__()
        self.client = consulate.Consul(server, token=token, datacenter=dc)

    def get(self, key):
        return self.client.kv.get(str(key))

    def set(self, key, value):
        return self.client.kv.set(str(key), str(value))

    def delete(self, key):
        return self.client.kv.delete(str(key), recurse=True)

    def sub_keys(self, key):
        key = key.lstrip('/').rstrip('/')
        key_parts = key.split('/')
        if key_parts == ['']:
            key_parts = []

        if len(key_parts) == 0:
            matching_keys = self.client.kv.find('')
        else:
            matching_keys = [k for k in self.client.kv.find(str(key)) if k.startswith(key + '/')]

        if len(matching_keys) == 0:
            return None
        else:
            return set([k.split('/')[len(key_parts)] for k in matching_keys])

    def items(self, prefix=None):
        for key in self.client.kv.find(prefix or ''):
            yield key, self.client.kv.get(key)

    def __str__(self):
        return 'consul key value server=%s' % self.client.status.leader()


