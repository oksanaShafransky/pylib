__author__ = 'Amit'

import json
from pycountry import countries
from pylib.sw_config.kv_factory import provider_from_config


# default_conf = {'pylib.sw_config.mock.DictProxy': {'services/mobile-web/env/main/countries': '840'}}


class MobileWebConfig(object):
    default_conf = """
              [
                {
                     "class": "pylib.sw_config.consul.ConsulProxy",
                     "server":"consul.service.production",
                },
                {
                     "class": "pylib.sw_config.etcd_kv.EtcdProxy",
                     "server":"etcd.service.production",
                     "port": 4001,
                     "root_path": "v1/production"
                }
              ]
    """

    def __init__(self, env='main', conf=default_conf):
        self.root = 'services/mobile-web/env/%s' % env
        self.conf = provider_from_config(conf)
        self._countries = {}

    @property
    def countries(self):
        if not self._countries:
            self._countries = dict([(country_code, countries.get(numeric='%s' % country_code.zfill(3)).alpha2)
                                    for country_code in self.conf.get('%s/countries' % self.root).split(',')])
        return self._countries
