__author__ = 'Felix'

from inspect import isclass
from copy import copy

from pylib.sw_config.kv import KeyValueProxy


class Override(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def invoke(self, fnc):
        return lambda x: self.value if x == self.key else fnc(x)

    def __call__(self, fnc):
        return self.invoke(fnc)


class WithSet(object):
    def __init__(self, key, value):
        self.modified_key = key
        self.modified_value = value

    @staticmethod
    def invoke(cls, modified_key, new_value):
        class Decorated(cls):
            def __init__(self, *args, **kwargs):
                cls.__init__(self, *args, **kwargs)
                self.get = Override(modified_key, new_value)(self.get)

        return Decorated

    def __call__(self, cls_or_instance):
        if isclass(cls_or_instance):
            return WithSet.invoke(cls_or_instance, self.modified_key, self.modified_value)
        else:
            ret = copy(cls_or_instance)
            ret.get = Override(self.modified_key, self.modified_value)(ret.get)
            return ret


class WithDelete(WithSet):
    def __init__(self, key, value):
        super(WithDelete, self).__init__(key, value)
        self.modified_key = key
        self.modified_value = None


class DictProxy(KeyValueProxy):
    def __init__(self, **kwargs):
        super(DictProxy, self).__init__()
        self.db = kwargs

    def get(self, key):
        return self.db.get(key, None)

    def set(self, key, value):
        self.db[key] = value

    def delete(self, key):
        self.db.__delitem__(key)

    def sub_keys(self, key):
        key_parts_len = len(key.split('/'))
        return set([dk.split('/')[key_parts_len] for dk in self.db.keys()
                    if dk.startswith(key + '/')])

