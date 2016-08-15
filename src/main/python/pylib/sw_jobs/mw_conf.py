__author__ = 'Amit'

from pycountry import countries


class MobileWebConfig:
    def __init__(self, env='main', is_local=False):
        self.root = 'services/mobile-web/env/%s' % env
        if is_local:
            from pylib.sw_config.mock import DictProxy
            countries_dict = {'%s/countries' % self.root: '840,826'}
            self.conf = DictProxy(**countries_dict)
            pass
        else:
            from pylib.sw_config.composite_kv import CompositeConfigurationProxy
            self.conf = CompositeConfigurationProxy()

        self._countries = {}

    @property
    def countries(self):
        if not self._countries:
            self._countries = dict([(country_code, countries.get(numeric='%s' % country_code.zfill(3)).alpha2)
                               for country_code in self.conf.get('%s/countries' % self.root).split(',')])
        return self._countries
