__author__ = 'Amit'

import pycountry
from pylib.sw_config.bigdata_kv import get_kv


# import sys ;logger.setLevel(logging.DEBUG); handler = logging.StreamHandler(sys.stdout) ;
# handler.setLevel( logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter) ; logger.addHandler(handler)

class MobileWebConfig(object):

    def __init__(self, mobile_web_env='main', kv_provider=None, snowflake_env=None):
        self.root = 'services/mobile-web/env/%s' % mobile_web_env
        self.conf = kv_provider or get_kv(snowflake_env=snowflake_env)
        self._countries = {}

    @property
    def countries(self):
        if not self._countries:
            self._countries = dict([(country_code, pycountry.countries.get(numeric='%s' % country_code.zfill(3)).alpha2)
                                    for country_code in self.conf.get('%s/countries' % self.root).split(',')])

        return self._countries
