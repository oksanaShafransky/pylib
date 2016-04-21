__author__ = 'Felix'

import sys
import logging

from common.dependency import register_instance
from mock import *
from kv import KeyValueProxy
from window_config import SimilarWebWindowConfig


ETCD_PATHS = {'production': 'v1/production', 'staging': 'v1/staging', 'dev': 'v1/dev'}
from etcd_kv import EtcdProxy
PROXY_CLASS = EtcdProxy

HEALTHY = 1
MINIMAL = 0


def init_env(env_type, changes=[], deletes=[]):
    effective_cls = PROXY_CLASS

    for key, value in changes:
        logging.info('simulating change of %s to %s' % (key, value))
        effective_cls = WithSet(key=key, value=value)(effective_cls)

    for key in deletes:
        logging.info('simulating delete of %s' % key)
        effective_cls = WithDelete(key)(effective_cls)

    register_instance(KeyValueProxy, effective_cls('etcd.service.production', root_path=ETCD_PATHS[env_type]))


def parse_modifications(args):
    sets, deletes = [], []

    idx = 0
    while idx < len(args):
        if args[idx] == '-s':
            sets += [(args[idx + 1], args[idx + 2])]
            idx += 3
        elif args[idx] == '-d':
            deletes += [args[idx + 1]]
            idx += 2
        else:
            idx += 1  # unknown option

    return sets, deletes


def check_config(settings_provider, env_type='production', sets=[], deletes=[], health_level=HEALTHY):
    init_env(env_type, changes=sets, deletes=deletes)

    success = True
    for name, artifact in settings_provider.get_artifacts().iteritems():
        num_dates = len(artifact.dates)
        if num_dates < settings_provider.min_viable_options():
            logging.error('%s is in a dangerous state with %d valid days' % (name, num_dates))
            success = False
        elif num_dates < settings_provider.min_healthy_options():
            logging.warn('%s is in danger with only %d valid days' % (name, num_dates))
            if health_level > MINIMAL:
                success = False
        else:
            logging.info('%s is OK with %d valid days' % (name, num_dates))

    return success

if __name__ == '__main__':

    # TODO add artifacts option and filter the ones provided by the config
    sets, deletes = parse_modifications(sys.argv[1:])
    if not check_config(SimilarWebWindowConfig, changes=sets, deletes=deletes):
        sys.exit(1)
