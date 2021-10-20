import json

from pylib.sw_config.consul import ConsulProxy
from pylib.sw_config.composite_kv import PrefixedConfigurationProxy
from pylib.sw_config.types import Purposes

snowflake_keys = {
    Purposes.BigData: 'bigdata-consul-kv',
    Purposes.Ingest: 'ingest-consul-kv',
    Purposes.WebProduction: 'web-production-consul-kv',
    Purposes.WebStaging: 'web-staging-consul-kv',
    Purposes.WebLocal: 'web-local-consul-kv',
    Purposes.DI: 'di-consul-kv',
    Purposes.DataFactory: 'df-consul-kv',
    Purposes.LiteEast: 'lite-east-consul-kv',
    Purposes.LiteWest: 'lite-west-consul-kv',
}


def get_kv(purpose=Purposes.BigData, snowflake_env=None, append_prefix=True, dc=None):
    # import snowflake config in function to allow mock in unit tests
    from pylib.config.SnowflakeConfig import SnowflakeConfig
    consul_snowflake_key = snowflake_keys[purpose]
    consul_properties_json = SnowflakeConfig(snowflake_env).get_service_name(service_name=consul_snowflake_key)
    consul_properties = json.loads(consul_properties_json)

    basic_kv = ConsulProxy(
        server=consul_properties['server'],
        token=consul_properties.get('token'),  # user .get to allow None token
        dc=dc
    )

    if not append_prefix or 'prefix' not in consul_properties:
        return basic_kv

    return PrefixedConfigurationProxy(
        underlying_proxy=basic_kv,
        prefixes=[consul_properties['prefix']]
    )
