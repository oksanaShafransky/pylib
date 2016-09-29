__author__ = 'Amitr'

from kv import KeyValueProxy


class CompositeConfigurationProxy(KeyValueProxy):
    def __init__(self, proxies):
        KeyValueProxy.__init__(self)
        self.proxies = proxies

    def get(self, key):
        for proxy in self.proxies:
            try:
                return proxy.get(key)
            except Exception:
                pass

        return None

    def set(self, key, value):
        for proxy in self.proxies:
            try:
                proxy.set(key, value)
            except Exception:
                pass

    def delete(self, key):
        for proxy in self.proxies:
            try:
                proxy.delete(key)
            except Exception:
                pass

    def sub_keys(self, key):
        for proxy in self.proxies:
            try:
                return proxy.sub_keys(key)
            except Exception:
                pass

        return None

