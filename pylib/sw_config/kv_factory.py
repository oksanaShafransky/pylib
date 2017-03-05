from pydoc import locate
import json

from composite_kv import CompositeConfigurationProxy


def create_proxy(proxy_cls, params):
    return locate(proxy_cls)(**params)


def provider_from_config(config):
    if isinstance(config, basestring):
        kv_conf = json.loads(config)
    else:
        kv_conf = config

    proxies = []
    if not isinstance(kv_conf, list):
        kv_conf = [kv_conf]
    for proxy_def in kv_conf:
        cls = proxy_def.pop('class')
        proxies += [create_proxy(cls, proxy_def)]

    return CompositeConfigurationProxy(proxies)


if __name__ == '__main__':
    conf = """
                  [
                    {
                         "class": "pylib.sw_config.consul.ConsulProxy",
                         "server":"consul.service.production"
                    },
                    {
                         "class": "pylib.sw_config.etcd_kv.EtcdProxy",
                         "server":"etcd.service.production",
                         "port": 4001,
                         "root_path": "v1/production"
                    }
                  ]
    """

    print provider_from_config(conf)


