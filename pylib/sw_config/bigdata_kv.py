from pylib.config.SnowflakeConfig import SnowflakeConfig
from pylib.sw_config.kv_factory import provider_from_config
from pylib.sw_config.composite_kv import PrefixedConfigurationProxy


class KeyValueConfig(object):
    consul_endpoint = SnowflakeConfig().get_service_name(service_name='consul')
    _kv_prod_conf = """
              [
                {
                     "class": "pylib.sw_config.consul.ConsulProxy",
                     "server":"{}"
                }
              ]
    """.format(consul_endpoint)

    base_kv = {
        'production': provider_from_config(_kv_prod_conf)
    }


def get_kv(env='production', purpose='bigdata'):
    basic_kv = KeyValueConfig.base_kv[env.lower()]
    return basic_kv if purpose is None else PrefixedConfigurationProxy(basic_kv, prefixes=[purpose])