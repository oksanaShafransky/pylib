__author__ = 'Amit'

import json
from pycountry import countries
from pylib.tasks.ptask_infra import TasksInfra


# default_conf = {'pylib.sw_config.mock.DictProxy': {'services/mobile-web/env/main/countries': '840'}}


class MobileWebConfig(object):

    def __init__(self, mobile_web_env='main', kv_provider=None):
        self.root = 'services/mobile-web/env/%s' % mobile_web_env
        self.conf = kv_provider or TasksInfra.kv()
        self._countries = {}

    @property
    def countries(self):
        if not self._countries:
            self._countries = dict([(country_code, countries.get(numeric='%s' % country_code.zfill(3)).alpha2)
                                    for country_code in self.conf.get('%s/countries' % self.root).split(',')])
        return self._countries
