__author__ = 'Felix'

import sys

from common.dependency import register_instance
from data import *
from mock import *
from kv import KeyValueProxy

ETCD_PATHS = {'production': 'v1/production', 'staging': 'v1/staging', 'dev': 'v1/dev'}
from etcd_kv import EtcdProxy
PROXY_CLASS = EtcdProxy

REQUIRED_DATES_SIZE = 3


def init_env(env_type, changes=[], deletes=[]):
    effective_cls = PROXY_CLASS

    for key, value in changes:
        effective_cls = WithSet(key=key, value=value)(effective_cls)

    for key in deletes:
        effective_cls = WithDelete(key)(effective_cls)

    register_instance(KeyValueProxy, effective_cls('etcd.service.production', root_path=ETCD_PATHS[env_type]))


def get_artifacts():
    desktop_window = Artifact('services/current-web-dates/window', required_value='true')
    mw_window = Artifact('services/current-mobile-web-dates/window', required_value='true')
    web_analysis = Intersect(desktop_window, mw_window)

    app_engagement = Artifact('services/mobile-usage-ranks/data-available/window')
    scraping = Artifact('services/process_mobile_scraping/data-available')
    top_apps = Intersect(app_engagement, scraping)

    return {'Web Analysis': web_analysis, 'Top Apps': top_apps}


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

if __name__ == '__main__':

    sets, deletes = parse_modifications(sys.argv[1:])
    init_env('production', changes=sets, deletes=deletes)

    for name, artifact in get_artifacts().iteritems():
        num_dates = len(artifact.dates)
        if num_dates < REQUIRED_DATES_SIZE:
            raise Exception('%s is in a dangerous state with %d valid days' % (name, num_dates))
        else:
            print '%s is OK with %d valid days' % (name, num_dates)