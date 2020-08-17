from pycountry import countries
from pylib.tasks.ptask_infra import TasksInfra


class AppsEngagementConfig(object):

    ALLOW_ALL = '*'

    def __init__(self, app_eng_env, kv_provider=None):
        self.conf = kv_provider or TasksInfra.kv()
        self.root = 'services/app-engagement/env/%s' % app_eng_env

        self._countries = {}
        self._default_sqs_decay_factor = None
        self._countries_sqs_decay_factor = {}
        self._base_env_confs = None
        self._device_weights_whitelist_per_country = None
        self._users_weights_whitelist_per_country = None

    def _parse_list_or_default(self, key):
        kv_val = self.conf.get(key)
        if kv_val == AppsEngagementConfig.ALLOW_ALL:
            return []
        else:
            return [elem for elem in kv_val.split(',') if elem not in [None, '']]

    @property
    def countries(self):
        # TODO check that all of the envs. updated to pycountry version 18.12.8 and remove this check
        #  REMOVE THE LOG!
        if not self._countries:
            if hasattr(countries.get(numeric='840'), 'alpha2'):
                print('\nusing pycountry v==1.2\n')
                self._countries = dict([(country_code, countries.get(numeric='%s' % country_code.zfill(3)).alpha2)
                                        for country_code in self._parse_list_or_default('%s/countries' % self.root)])
            else:
                print('\nusing pycountry v==18.12.8\n')
                self._countries = dict([(country_code, countries.get(numeric='%s' % country_code.zfill(3)).alpha_2)
                                        for country_code in self._parse_list_or_default('%s/countries' % self.root)])
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

    @property
    def device_weights_whitelist_per_country(self):

        if self._device_weights_whitelist_per_country is None:

            self._device_weights_whitelist_per_country = dict([(key, ','.join(
                self._parse_list_or_default('%s/devices_sqs_whitelist/%s' % (self.root, key))))
                        for key in self.conf.sub_keys('%s/devices_sqs_whitelist' % self.root)])
            return self._device_weights_whitelist_per_country

    @property
    def users_weights_whitelist_per_country(self):

        if self._users_weights_whitelist_per_country is None:

            self._users_weights_whitelist_per_country = dict([(key, ','.join(
                self._parse_list_or_default('%s/users_sqs_whitelist/%s' % (self.root, key))))
                        for key in self.conf.sub_keys('%s/users_sqs_whitelist' % self.root)])
            return self._users_weights_whitelist_per_country
