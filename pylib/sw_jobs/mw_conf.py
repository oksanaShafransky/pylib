__author__ = 'Amit'

from pycountry import countries
from pylib.sw_config.bigdata_kv import get_kv
import logging

logger = logging.getLogger("mw_conf")

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
            # TODO check that all of the envs. updated to pycontry version 18.12.8 and remove this check
            #  REMOVE THE LOG!
            if hasattr(countries.get(numeric='840'), 'alpha2'):
                logger.debug('using pycountry v==1.2')
                self._countries = dict([(country_code, countries.get(numeric='%s' % country_code.zfill(3)).alpha2)
                                        for country_code in self.conf.get('%s/countries' % self.root).split(',')])
            else:
                logger.debug('using pycountry v==18.12.8')
                self._countries = dict([(country_code, countries.get(numeric='%s' % country_code.zfill(3)).alpha_2)
                                        for country_code in self.conf.get('%s/countries' % self.root).split(',')])

        return self._countries



