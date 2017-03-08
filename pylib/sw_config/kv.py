__author__ = 'Felix'


#Defines the api for key/value proxies
class KeyValueProxy(object):
    def __init__(self):
        pass

    def get(self, key):
        raise NotImplementedError()

    def get_or_default(self, key, default_value=None):
        try:
            return self.get(key)
        except Exception:
            return default_value

    def set(self, key, value):
        raise NotImplementedError()

    def delete(self, key):
        raise NotImplementedError()

    # immediate sub keys only, with relative names
    def sub_keys(self, key):
        raise NotImplementedError()

    # yield key,value pairs
    def items(self, prefix=None):
        raise NotImplementedError()
