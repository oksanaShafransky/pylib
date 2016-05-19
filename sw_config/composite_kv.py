__author__ = 'Amitr'

from etcd_kv import EtcdProxy
from consul import ConsulProxy
from kv import KeyValueProxy

ETCD_PATHS = {'production': 'v1/production', 'staging': 'v1/staging', 'dev': 'v1/dev'}


class CompositeConfigurationProxy(KeyValueProxy):
    def __init__(self, env='production'):
        KeyValueProxy.__init__(self)
        self.consul = ConsulProxy(env.lower())
        self.etcd = EtcdProxy('etcd.service.production', root_path=ETCD_PATHS[env.lower()])

    def get(self, key):
        try:
            return self.etcd.get(key)
        except Exception:
            return self.consul.get(key)

    def set(self, key, value):
        try:
            self.etcd.set(key, value)
        except Exception:
            pass
        self.consul.set(key, value)

    def delete(self, key):
        try:
            self.etcd.delete(key)
        except Exception:
            pass
        self.consul.delete(key)

    def sub_keys(self, key):
        try:
            return self.etcd.sub_keys(key)
        except Exception:
            return self.consul.sub_keys(key)
