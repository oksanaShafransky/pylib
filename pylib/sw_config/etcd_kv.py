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

    def _get_raw(self, key):
        return self.client.get(self._full_path(str(key)))

    def get(self, key):
        return str(self._get_raw(key).value)

    def set(self, key, value):
        try:
            return self.client.set(self._full_path(str(key)), str(value))
        except:
            import traceback
            traceback.print_exc()

    def delete(self, key):
        return self.client.delete(self._full_path(str(key)))

    def sub_keys(self, key):
        key_parts = self._full_path(key).split('/')
        key_parts = [kp for kp in key_parts if kp != '']

        res = self.client.get(self._full_path(str(key)))
        if len(res._children) == 0:  # there is a bug in the client causing an empty dir to return itself as a child
            return set()

        sub_nodes = res.children
        # the +1 is due to / always preceding the key, yielding an extra element
        return set([sn.key.split('/')[len(key_parts) + 1] for sn in sub_nodes if sn.key != self._full_path(key)])

    def items(self, prefix=None):
        next_keys = [prefix or '/']
        while len(next_keys) > 0:
            nk = next_keys.pop(0)
            try_val = self._get_raw(nk)
            if not try_val.dir:
                yield nk.replace('//', '/'), str(try_val.value)
            else:
                for sk in self.sub_keys(nk):
                    next_keys.insert(0, '%s/%s' % (nk, sk))

    def __str__(self):
        return 'etcd key value server=%s, port=%d, root_path=%s' % (self.client.host, self.client.port, self.root_path)


