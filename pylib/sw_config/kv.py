__author__ = 'Felix'


#Defines the api for key/value proxies
class KeyValueProxy(object):
    def __init__(self):
        pass

    def get(self, key):
        raise NotImplementedError()

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
