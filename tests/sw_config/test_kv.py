import json

from pylib.sw_config.kv import KeyValueProxy

from pylib.sw_config.composite_kv import PrefixedConfigurationProxy
from pylib.sw_config.dict_change_simulator import DictProxy
from pylib.sw_config.kv_tools import kv_to_tree, load_kv, kv_diff, KeyValueTree
from pylib.sw_config.types import Purposes
import unittest

import pylib.sw_config.consul


class ConsulProxyMock(KeyValueProxy):
    def __init__(self, server, token, dc):
        super(ConsulProxyMock, self).__init__()
        self.server = server
        self.token = token
        self.dc = dc


pylib.sw_config.consul.ConsulProxy = ConsulProxyMock


class TestTools(unittest.TestCase):
    def test_list(self):
        example_dict = {
            '/a': 12, '/b': 6, '/c': 19
        }
        proxy = DictProxy(**example_dict)
        assert example_dict == dict(proxy.items())

    def test_subtree(self):
        example_dict = {
            '/a/b': 12, '/a/c': 6, '/b/b': 19
        }
        proxy = DictProxy(**example_dict)
        a_tree = proxy.items('/a')
        example_dict.pop('/b/b')
        assert example_dict == dict(a_tree)

    def test_save_load(self):
        import json

        example_dict = {
            '/a/b': 12, '/a/c': 6, '/b/b': 19
        }

        original = DictProxy(**example_dict)
        kv_tree = kv_to_tree(original)
        str_repr = json.dumps(kv_tree.root)

        deserialized = DictProxy()
        load_kv(deserialized, str_repr)

        assert original.db == deserialized.db

    def test_diff_one_sided(self):
        dict_a = {
            '/a/b': 12, '/a/c': 6, '/b/b': 19
        }
        kv_a = DictProxy(**dict_a)

        dict_b = {
            '/a/c': 5, '/b/b': 19, '/d/c': 11
        }
        kv_b = DictProxy(**dict_b)

        assert len(kv_diff(kv_a, kv_b, two_sided=False)) == 2

    def test_diff_two_sided(self):
        dict_a = {
            '/a/b': 12, '/a/c': 6, '/b/b': 19
        }
        kv_a = DictProxy(**dict_a)

        dict_b = {
            '/a/c': 5, '/b/b': 19, '/d/c': 11
        }
        kv_b = DictProxy(**dict_b)

        assert len(kv_diff(kv_a, kv_b, two_sided=True)) == 3


class TestPrefixedConfiguration(object):
    def test_no_prefix(self):
        original_key = 'key/full/path'

        class UnderlyingProxy(object):
            def get(self, key):
                if key == original_key:
                    return True
                else:
                    return None

        proxy = PrefixedConfigurationProxy(UnderlyingProxy(), [])
        assert proxy.get(original_key)

    def test_none_prefix(self):
        original_key = 'key/full/path'

        class UnderlyingProxy(object):
            def get(self, key):
                if key == original_key:
                    return True
                else:
                    return None

        proxy = PrefixedConfigurationProxy(UnderlyingProxy())
        assert proxy.get(original_key)

    def test_prefix_not_found(self):
        original_key = 'key/full/path'

        class UnderlyingProxy(object):
            def get(self, key):
                if key == original_key:
                    return True
                if key == 'prefix/%s' % original_key:
                    return None

        proxy = PrefixedConfigurationProxy(UnderlyingProxy(), ['prefix'])
        assert proxy.get(original_key) is None

    def test_prefix_found(self):
        original_key = 'key/full/path'

        class UnderlyingProxy(object):
            def get(self, key):
                if key == original_key:
                    return None
                if key == 'prefix/%s' % original_key:
                    return True

        proxy = PrefixedConfigurationProxy(UnderlyingProxy(), ['prefix'])
        assert proxy.get(original_key)

    def test_prefix_with_slash(self):
        original_key = 'key/full/path'

        class UnderlyingProxy(object):
            def get(self, key):
                if key == original_key:
                    return None
                if key == 'prefix/with/slash/%s' % original_key:
                    return True

        proxy = PrefixedConfigurationProxy(UnderlyingProxy(), ['prefix/with/slash'])
        assert proxy.get(original_key)

    def test_multiple_prefixes(self):
        original_key = 'key/full/path'

        class UnderlyingProxy(object):
            def get(self, key):
                if key == original_key:
                    return None
                if key == 'prefix1/prefix2/%s' % original_key:
                    return True

        proxy = PrefixedConfigurationProxy(UnderlyingProxy(), ['prefix1', 'prefix2'])
        assert proxy.get(original_key)


class TestGetKV(unittest.TestCase):
    def test_kv_with_token(self):
        class SnowflakeConfigMock(object):
            def __init__(self, env):
                assert env == 'test_env'

            def get_service_name(self, service_name):
                return '{"server": "test_server", "token": "test_token"}'

        import pylib.config.SnowflakeConfig
        pylib.config.SnowflakeConfig.SnowflakeConfig = SnowflakeConfigMock
        from pylib.sw_config.bigdata_kv import get_kv
        kv = get_kv(purpose=Purposes.BigData, snowflake_env='test_env')
        assert kv.server == 'test_server'
        assert kv.token == 'test_token'

    def test_kv_without_token(self):
        class SnowflakeConfigMock(object):
            def __init__(self, env):
                assert env == 'test_env'

            def get_service_name(self, service_name):
                return '{"server": "test_server"}'

        import pylib.config.SnowflakeConfig
        pylib.config.SnowflakeConfig.SnowflakeConfig = SnowflakeConfigMock
        from pylib.sw_config.bigdata_kv import get_kv
        kv = get_kv(purpose=Purposes.BigData, snowflake_env='test_env')
        assert kv.server == 'test_server'
        assert kv.token is None

    def test_purpose(self):
        class SnowflakeConfigMock(object):
            def __init__(self, env):
                assert env == 'test_env'

            def get_service_name(self, service_name):
                assert service_name == 'bigdata-consul-kv'
                return '{"server": "test_server"}'

        import pylib.config.SnowflakeConfig
        pylib.config.SnowflakeConfig.SnowflakeConfig = SnowflakeConfigMock
        from pylib.sw_config.bigdata_kv import get_kv
        kv = get_kv(purpose=Purposes.BigData, snowflake_env='test_env')

    def test_kv_with_prefix(self):
        class SnowflakeConfigMock(object):
            def __init__(self, env):
                assert env == 'test_env'

            def get_service_name(self, service_name):
                return '{"server": "test_server", "prefix": "prefix1/prefix2"}'

        import pylib.config.SnowflakeConfig
        pylib.config.SnowflakeConfig.SnowflakeConfig = SnowflakeConfigMock
        from pylib.sw_config.bigdata_kv import get_kv
        kv = get_kv(purpose=Purposes.BigData, snowflake_env='test_env')
        assert type(kv).__name__ == 'PrefixedConfigurationProxy'
        assert kv.prefix == 'prefix1/prefix2/'

    def test_kv_without_prefix(self):
        class SnowflakeConfigMock(object):
            def __init__(self, env):
                assert env == 'test_env'

            def get_service_name(self, service_name):
                return '{"server": "test_server"}'

        import pylib.config.SnowflakeConfig
        pylib.config.SnowflakeConfig.SnowflakeConfig = SnowflakeConfigMock
        from pylib.sw_config.bigdata_kv import get_kv
        kv = get_kv(purpose=Purposes.BigData, snowflake_env='test_env')
        assert type(kv).__name__ == 'ConsulProxyMock'

    def test_kv_ignore_prefix(self):
        class SnowflakeConfigMock(object):
            def __init__(self, env):
                assert env == 'test_env'

            def get_service_name(self, service_name):
                return '{"server": "test_server", "prefix": "prefix1/prefix2"}'

        import pylib.config.SnowflakeConfig
        pylib.config.SnowflakeConfig.SnowflakeConfig = SnowflakeConfigMock
        from pylib.sw_config.bigdata_kv import get_kv
        kv = get_kv(purpose=Purposes.BigData, snowflake_env='test_env', append_prefix=False)
        assert type(kv).__name__ == 'ConsulProxyMock'

    def test_valid_purpose(self):
        class SnowflakeConfigMock(object):
            def __init__(self, env):
                pass

            def get_service_name(self, service_name):
                return '{"server": "test_server", "prefix": "prefix1/prefix2"}'

        import pylib.config.SnowflakeConfig
        pylib.config.SnowflakeConfig.SnowflakeConfig = SnowflakeConfigMock
        from pylib.sw_config.bigdata_kv import get_kv
        # assert that calling get)kv with invalid purpose yields exception
        try:
            kv = get_kv(purpose='invalid_purpose', snowflake_env='test_env')
            assert False
        except Exception:
            assert True


class TestPurposes(object):
    def test_web_production(self):
        assert Purposes.get_web_purpose('production') == Purposes.WebProduction

    def test_web_staging(self):
        assert Purposes.get_web_purpose('staging') == Purposes.WebStaging

    def test_case_insensitivity(self):
        assert Purposes.get_web_purpose('PRODUCTION') == Purposes.WebProduction

    def test_invalid_env(self):
        try:
            Purposes.get_web_purpose('invalid')
            assert False
        except:
            assert True


class TestKeyValueTree(object):
    def test_get_value(self):
        kvt = KeyValueTree()
        kvt.add_kv('folder1/folder2/key1', 'v')
        assert kvt.get_value('folder1/folder2/key1') == 'v'
        assert kvt.get_value('folder1/non/existing') is None

    def test_len(self):
        kvt = KeyValueTree()
        kvt.add_kv('folder1/folder2/key1', 'v')
        kvt.add_kv('folder1/folder2/key2', 'v')
        kvt.add_kv('folder1/folder3/key2', 'v')
        kvt.add_kv('folder1/key3', 'v')
        assert len(kvt) == 4

    def test_iteration(self):
        kvt = KeyValueTree()
        kvt.add_kv('folder1/folder2/key1', 'v')
        kvt.add_kv('folder1/folder2/key2', 'v')
        kvt.add_kv('folder1/folder3/key2', 'v')
        kvt.add_kv('folder1/key3', 'v')

        count = 0
        for kv in kvt:
            count += 1

        assert count == 4

    def test_leaf_branch(self):
        kvt = KeyValueTree()
        kvt.add_kv('folder1/leaf_folder', 'v1')
        kvt.add_kv('folder1/leaf_folder/key', 'v2')

        assert kvt.get_value('folder1/leaf_folder') == 'v1'
        assert kvt.get_value('folder1/leaf_folder/key') == 'v2'

    def test_leaf_branch2(self):
        kvt = KeyValueTree()
        kvt.add_kv('folder1/leaf_folder/key', 'v2')
        kvt.add_kv('folder1/leaf_folder', 'v1')

        assert kvt.get_value('folder1/leaf_folder') == 'v1'
        assert kvt.get_value('folder1/leaf_folder/key') == 'v2'

    def test_init_from_json(self):
        kvt = KeyValueTree()
        kvt.add_kv('p1/p2', 'v1')
        kvt.add_kv('p1/p2/p3', 'v2')
        kvt.add_kv('p1/p2/p3/p4', 'v3')
        kvt.add_kv('p4/p5/p6', 'v4')
        kvt.add_kv('p4/p5/p7', 'v5')

        j = json.dumps(kvt.root)

        kvt2 = KeyValueTree(j)
        assert len(kvt2) == 5
        assert kvt2.get_value('p1/p2') == 'v1'
        assert kvt2.get_value('p1/p2/p3') == 'v2'
        assert kvt2.get_value('p1/p2/p3/p4') == 'v3'
        assert kvt2.get_value('p4/p5/p6') == 'v4'
        assert kvt2.get_value('p4/p5/p7') == 'v5'
        assert kvt2.get_value('non/existing') is None

    def test_ignore_folder_and_empty(self):
        class MockKv(object):
            def items(self, prefix):
                return [
                    (u'', None),
                    (u'production/', None),
                    (u'production/last_update/internal_do_not_watch/redis', u'aa')
                ]

        tree = kv_to_tree(MockKv())
        assert len(tree) == 1


if __name__ == '__main__':
    unittest.main()
