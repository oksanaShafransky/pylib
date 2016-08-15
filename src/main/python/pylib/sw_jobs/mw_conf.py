__author__ = 'Amit'

from pycountry import countries

from airflow.models import Variable

from pylib.sw_config.kv_factory import provider_from_config


class MobileWebConfig:
    def __init__(self, env='main'):
        self.root = 'services/mobile-web/env/%s' % env
        self.conf = provider_from_config(Variable.get('key_value_production'))
        self._countries = {}

    @property
    def countries(self):
        if not self._countries:
            self._countries = dict([(country_code, countries.get(numeric='%s' % country_code.zfill(3)).alpha2)
                               for country_code in self.conf.get('%s/countries' % self.root).split(',')])
        return self._countries
