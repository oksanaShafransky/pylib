__author__ = 'Amitr'

from kv import KeyValueProxy


class CompositeConfigurationProxy(KeyValueProxy):
    def __init__(self, proxies):
        KeyValueProxy.__init__(self)
        self.proxies = proxies

    def set(self, key, value):
        for proxy in self.proxies:
            proxy.set(key, value)

    def delete(self, key):
        for proxy in self.proxies:
            try:
                proxy.delete(key)
            except Exception:
                pass

    def sub_keys(self, key):
        return self.__fetch_and_assert_equality(lambda proxy, key: proxy.sub_keys(key), key,
                                                lambda val1, val2: set(val1) == set(val2))

    def get(self, key):
        return self.__fetch_and_assert_equality((lambda proxy, key: proxy.get(key)), key)

    def __catch(self, func):
        try:
            return func()
        except Exception as e:
            return None

    def __fetch_and_assert_equality(self, func, key, cmprtr=lambda val1, val2: val1 == val2):
        # Make sure that values across providers are equal (eliminate Nones)
        values = [item for item in [self.__catch(lambda: func(proxy, key)) for proxy in self.proxies] if
                  ((item is not None) and (item != []))]

        # If the result list is empty, that means that the values across proxies are all None (don't exist in the KV)
        if len(values) != 0:
            #assert all(
            #    [cmprtr(values[0], item) for item in values]), "Values for key %s are not equal across providers" % key
            return values[0]
        else:
            return None

    def items(self, prefix=None):
        detected_keys = set()
        for proxy in self.proxies:
            for key, val in proxy.items(prefix=prefix):
                if key not in detected_keys:
                    yield key, val

    def __str__(self):
        return 'composite key value\n%s' % '\n'.join([str(proxy) for proxy in self.proxies])


class PrefixedConfigurationProxy(KeyValueProxy):
    def __init__(self, underlying_proxy, prefixes):
        self.proxy = underlying_proxy
        self.prefix = '/'.join([pref for pref in prefixes if pref is not None])
        if len(self.prefix) > 0:
            self.prefix += '/'

    def get(self, key):
        return self.proxy.get('%s%s' % (self.prefix, key))

    def set(self, key, value):
        return self.proxy.set('%s%s' % (self.prefix, key), value)

    def delete(self, key):
        return self.proxy.delete('%s%s' % (self.prefix, key))

    def sub_keys(self, key):
        return self.proxy.sub_keys('%s%s' % (self.prefix, key))

    def items(self, prefix=None):
        return map(lambda key_val: (key_val[0].lstrip(self.prefix), key_val[1]),
                   self.proxy.items(prefix=self.prefix+(prefix or '')))

    def __str__(self):
        return '%s at branch %s' % (self.proxy, self.prefix)


