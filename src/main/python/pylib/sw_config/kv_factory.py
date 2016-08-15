__author__ = 'Felix'

import json

from composite_kv import CompositeConfigurationProxy


def _load_class(cls):
    name_parts = cls.split('.')
    curr_mod = __import__(name_parts[0])
    for sub_mod in name_parts[1:]:
        curr_mod = getattr(curr_mod, sub_mod)

    return curr_mod


def create_proxy(proxy_cls, params):
    return _load_class(proxy_cls)(**params)


def provider_from_config(config_str):
    conf = json.loads(config_str)
    proxies = []
    for cls, params in conf.iteritems():
        proxies += [create_proxy(cls, params)]

    return CompositeConfigurationProxy(proxies)

