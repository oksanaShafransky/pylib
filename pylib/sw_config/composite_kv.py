__author__ = 'Amitr'

from kv import KeyValueProxy


class CompositeConfigurationProxy(KeyValueProxy):
    def __init__(self, proxies):
        KeyValueProxy.__init__(self)
        self.proxies = proxies

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
                  ((item is not None) and (item != [])) ]

        # If the result list is empty, that means that the values across proxies are all None (don't exist in the KV)
        if len(values) != 0:
            assert all(
                [cmprtr(values[0], item) for item in values]), "Values for key %s are not equal across providers" % key
            return values[0]
        else:
            return None

    def __str__(self):
        return 'composite key value\n%s' % '\n'.join([str(proxy) for proxy in self.proxies])
