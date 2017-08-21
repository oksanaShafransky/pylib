import logging

import six
import sys

from dict_change_simulator import WithDelete, WithSet
from etcd_kv import EtcdProxy
from window_config import SimilarWebWindowConfig

__author__ = 'Felix'

ETCD_PATHS = {'production': 'v1/production', 'staging': 'v1/staging', 'dev': 'v1/dev'}
PROXY_CLASS = EtcdProxy

HEALTHY = 1
MINIMAL = 0


def setup_simulation(kv_proxy, changes=None, deletes=None):
    deletes = deletes or []
    changes = changes or []

    for key, value in changes:
        logging.info('simulating change of %s to %s' % (key, value))
        kv_proxy = WithSet(key=key, value=value)(kv_proxy)

    for key in deletes:
        logging.info('simulating delete of %s' % key)
        kv_proxy = WithDelete(key=key, value=None)(kv_proxy)


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


def check_config(settings_provider, base_kv, sets=None, deletes=None, health_level=HEALTHY):
    deletes = deletes or []
    sets = sets or []

    setup_simulation(base_kv, changes=sets, deletes=deletes)

    success = True
    for name, artifact in six.iteritems(settings_provider.get_artifacts_filtered(base_kv, deletes)):
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

    from pylib.sw_config.kv_factory import provider_from_config
    test_conf = provider_from_config("""
        [
            {
                "class": "pylib.sw_config.consul.ConsulProxy",
                "server":"consul.service.production"
            }
        ]
    """)

    from pylib.sw_config.composite_kv import PrefixedConfigurationProxy
    wrapped_kv = PrefixedConfigurationProxy(test_conf, ['web', 'production'])

    if not check_config(SimilarWebWindowConfig, wrapped_kv, sets=sets, deletes=deletes):
        print 'check failed'
        sys.exit(1)
    else:
        print 'config would be fine'
