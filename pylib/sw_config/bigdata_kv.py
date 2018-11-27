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
    consul_host = SnowflakeConfig(snowflake_env).get_service_name(service_name="consul-kv")
    consul_token = SnowflakeConfig(snowflake_env).get_service_name(service_name="consul-kv.token")
    if consul_token == 'no-token':
        consul_token = None

    basic_kv = provider_from_config({
        'class': "pylib.sw_config.consul.ConsulProxy",
        'server': consul_host,
        "token": consul_token
    })

    # in case of bigdata/production we want to allow fallback to bigdata (since production keys may
    # not have copied to "production" sub-folder)
    if purpose == 'bigdata' and env == 'production':
        prefixes = [purpose]
        optional_get_prefixes = [env]
    else:
        prefixes = [purpose, env]
        optional_get_prefixes = None

    return basic_kv if purpose is None else PrefixedConfigurationProxy(
        underlying_proxy=basic_kv,
        prefixes=prefixes,
        optional_get_prefixes=optional_get_prefixes
    )
