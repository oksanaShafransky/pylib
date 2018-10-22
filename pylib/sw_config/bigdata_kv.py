from pylib.sw_config.kv_factory import provider_from_config
from pylib.sw_config.composite_kv import PrefixedConfigurationProxy
from pylib.config.SnowflakeConfig import SnowflakeConfig


class KeyValueConfig(object):
    def __init__(self, snowflake_env):
        consul_host = SnowflakeConfig(snowflake_env).get_service_name(service_name="consul-kv")
        consul_token = SnowflakeConfig(snowflake_env).get_service_name(service_name="consul-kv.token")
        if consul_token == 'no-token':
            consul_token = None


        self._kv_prod_conf = [{'class': "pylib.sw_config.consul.ConsulProxy",
                               'server': consul_host,
                               "token": consul_token}]
        self._kv_stage_conf = """
                      [
                        {
                             "class": "pylib.sw_config.consul.ConsulProxy",
                             "server":"consul.service.staging"
                        }
                      ]
            """

    def get_base_kv(self):
        return {
        'production': provider_from_config(self._kv_prod_conf),
        'staging': provider_from_config(self._kv_stage_conf)}


def get_kv(env='production', purpose='bigdata', snowflake_env=None):
    basic_kv = KeyValueConfig(snowflake_env).get_base_kv()[env.lower()]
    return basic_kv if purpose is None else PrefixedConfigurationProxy(basic_kv, prefixes=[purpose])
