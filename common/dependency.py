__author__ = 'Felix'

from collections import defaultdict

dependencies = defaultdict(dict)
instances = defaultdict(dict)


def get_class(service, key=None):
    return dependencies[service][key]


def get_instance(service, key=None):
    registered_instance = instances[service][key]
    return registered_instance if registered_instance is not None else get_class(service, key)()


def register_class(service, provider_cls, key=None):
    dependencies[service][key] = provider_cls


def register_instance(service, provider, key=None):
    if not isinstance(provider, service):
        raise TypeError()
    else:
        instances[service][key] = provider