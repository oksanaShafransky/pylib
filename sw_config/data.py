__author__ = 'Felix'

from common.dependency import get_instance
from kv import KeyValueProxy


def get_proxy():
    return get_instance(KeyValueProxy)

from datetime import datetime


class Artifact:
    def __init__(self, root_path, required_value='success', date_fmt='%Y-%m-%d'):
        self.proxy = get_proxy()
        self.root = root_path
        self.req_val = required_value
        self.fmt = date_fmt

    @property
    def dates(self):
        return [datetime.strptime(key, self.fmt) for key in self.proxy.sub_keys(self.root) if self.proxy.get('%s/%s' % (self.root, key)) == self.req_val]


class Intersect:
    def __init__(self, *args):
        self.sub_artifacts = args

    @property
    def dates(self):
        return list(set.intersection(*[set(arg.dates) for arg in self.sub_artifacts]))

