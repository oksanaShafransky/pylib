__author__ = 'Felix'

from pydoc import locate
import json

from composite_kv import CompositeConfigurationProxy


def create_proxy(proxy_cls, params):
    return locate(proxy_cls)(**params)


def provider_from_config(config_str):
    conf = json.loads(config_str)
    proxies = []
    for proxy_def in conf:
        cls = proxy_def.pop('class')
        proxies += [create_proxy(cls, proxy_def)]

    return CompositeConfigurationProxy(proxies)

