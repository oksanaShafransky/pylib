import json
from pydoc import locate

from composite_kv import CompositeConfigurationProxy
from airflow.models import Variable
from pylib.config.SnowflakeConfig import SnowflakeConfig


def is_config(anything):
    return isinstance(anything, dict) and 'class' in anything


def propagate_config(config_params):
    return dict(map(lambda kv: (kv[0], provider_from_config(kv[1]) if is_config(kv[1]) else kv[1]), config_params.items()))


def create_proxy(proxy_cls, params):
    return locate(proxy_cls)(**propagate_config(params))


def provider_from_config(config):
    if isinstance(config, basestring):
        kv_conf = json.loads(config)
    else:
        kv_conf = config

    if not isinstance(kv_conf, list):
        cls = kv_conf.pop('class')
        return create_proxy(cls, kv_conf)
    else:
        proxies = []
        for proxy_def in kv_conf:
            cls = proxy_def.pop('class')
            proxies += [create_proxy(cls, proxy_def)]

        return CompositeConfigurationProxy(proxies)


if __name__ == '__main__':
    conf = """
                  [
                    {
                         "class": "pylib.sw_config.consul.ConsulProxy",
                         "server":%(consul_name)s
                    }
                  ]
    """

    cur_consul = SnowflakeConfig().get_service_name(env=Variable.get(key='snowflake_env', default_var='mrp-test'),
                                                    service_name='consul')
    conf = conf % {'consul_name': cur_consul}

    print provider_from_config(conf)


