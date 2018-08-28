from pylib.sw_config.kv_factory import provider_from_config
from pylib.sw_config.composite_kv import PrefixedConfigurationProxy
from pylib.config.SnowflakeConfig import SnowflakeConfig


class KeyValueConfig(object):
    _kv_prod_conf = """
              [
                {{
                     "class": "pylib.sw_config.consul.ConsulProxy",
                     "server":{consul}
                }}
              ]
    """
    params = {'consul': SnowflakeConfig().get_service_name(service_name="consul")}
    _kv_prod_conf = _kv_prod_conf.format(**params)

    _kv_stage_conf = """
                  [
                    {
                         "class": "pylib.sw_config.consul.ConsulProxy",
                         "server":"consul.service.staging"
                    }
                  ]
        """

    base_kv = {
        'production': provider_from_config(_kv_prod_conf),
        'staging': provider_from_config(_kv_stage_conf)
    }


def get_kv(env='production', purpose='bigdata'):
    basic_kv = KeyValueConfig.base_kv[env.lower()]
    return basic_kv if purpose is None else PrefixedConfigurationProxy(basic_kv, prefixes=[purpose])