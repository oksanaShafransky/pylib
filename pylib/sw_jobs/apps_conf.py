from pylib.sw_config.kv_factory import provider_from_config
from pycountry import countries
#from airflow.models import Variable


class AppsEngagementConfig(object):
    def __init__(self, env):
        self.root = 'services/app-engagement/env/%s' % env
        # conf = Variable.get('key_value_production')
        conf = """{
             "pylib.sw_config.consul.ConsulProxy": {
                 "server":"consul.service.production"
             },
             "pylib.sw_config.etcd_kv.EtcdProxy": {
                 "server":"etcd.service.production",
                 "port": 4001,
                 "root_path": "v1/production"
             }
             }"""
        self.conf = provider_from_config(conf)

        self._countries = {}
        self._default_sqs_decay_factor = None
        self._countries_sqs_decay_factor = {}
        self._base_env_confs = None

    @property
    def countries(self):
        if not self._countries:
            self._countries = dict([(country_code, countries.get(numeric='%s' % country_code.zfill(3)).alpha2)
                               for country_code in self.conf.get('%s/countries' % self.root).split(',') if country_code not in [None,'']])
        return self._countries

    @property
    def default_sqs_decay_factor(self):
        if self._default_sqs_decay_factor is None:
            self._default_sqs_decay_factor = self.conf.get('%s/decay_factor/default' % self.root)
            print('df decay is %s' % self._default_sqs_decay_factor)
        return self._default_sqs_decay_factor

    @property
    def countries_sqs_decay_factor(self):
        countries = self.countries
        if self._countries_sqs_decay_factor is None:
            self._countries_sqs_decay_factor = dict([(country_code, self.conf.get('%s/decay_factor/%s' % (self.root, country_code)))
                                    for country_code in countries])
        return self._countries_sqs_decay_factor

    @property
    def default_sqs_smoothing_ssm(self):
        if self._default_sqs_decay_factor is None:
            self._default_sqs_decay_factor = self.conf.get('%s/ssm/default' % self.root)
            print('ssm is %s' % self._default_sqs_decay_factor)
        return self._default_sqs_decay_factor

    @property
    def countries_sqs_smoothing_ssm(self):
        countries = self.countries
        if self._countries_sqs_decay_factor is None:
            self._countries_sqs_decay_factor = dict([(country_code, self.conf.get('%s/ssm/%s' % (self.root, country_code)))
                                                     for country_code in countries])
        return self._countries_sqs_decay_factor

    @property
    def base_env_confs(self):

        if self._base_env_confs is None:
            self._base_env_confs = dict([(key, self.conf.get('%s/%s' % (self.root, key)))
                                         for key in self.conf.sub_keys('%s' % self.root)])
        return self._base_env_confs