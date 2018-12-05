import json

from pylib.sw_config.consul import ConsulProxy
from pylib.sw_config.composite_kv import PrefixedConfigurationProxy


def get_kv(purpose='bigdata', snowflake_env=None, append_prefix=True):
    # import snowflake config in function to allow mock in unit tests
    from pylib.config.SnowflakeConfig import SnowflakeConfig
    consul_snowflake_key = purpose + '-consul-kv'
    consul_properties_json = SnowflakeConfig(snowflake_env).get_service_name(service_name=consul_snowflake_key)
    consul_properties = json.loads(consul_properties_json)

    basic_kv = ConsulProxy(
        server=consul_properties['server'],
        token=consul_properties.get('token')  # user .get to allow None token
    )

    if not append_prefix or 'prefix' not in consul_properties:
        return basic_kv

    return PrefixedConfigurationProxy(
        underlying_proxy=basic_kv,
        prefixes=[consul_properties['prefix']]
    )
