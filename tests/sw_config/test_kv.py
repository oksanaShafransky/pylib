from pylib.sw_config.composite_kv import PrefixedConfigurationProxy
from pylib.sw_config.dict_change_simulator import DictProxy
from pylib.sw_config.kv_tools import KeyValueTree, kv_to_tree, load_kv, kv_diff


class TestTools(object):

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

    def test_optional_prefix_exists(self):
        original_key = 'key/full/path'

        class UnderlyingProxy(object):
            def get(self, key):
                if key == original_key:
                    return None
                if key == 'prefix/%s' % original_key:
                    return None
                if key == 'prefix/optional_prefix/key/full/path':
                    return 'optional_prefix'

        proxy = PrefixedConfigurationProxy(UnderlyingProxy(), ['prefix'], ['optional_prefix'])
        assert proxy.get(original_key) == 'optional_prefix'

    def test_multiple_optional_prefix_exists(self):
        original_key = 'key/full/path'

        class UnderlyingProxy(object):
            def get(self, key):
                if key == original_key:
                    return None
                if key == 'prefix/%s' % original_key:
                    return None
                if key == 'prefix/optional_prefix1/optional_prefix2/key/full/path':
                    return 'optional_prefix'

        proxy = PrefixedConfigurationProxy(UnderlyingProxy(), ['prefix'], ['optional_prefix1', 'optional_prefix2'])
        assert proxy.get(original_key) == 'optional_prefix'

    def test_optional_prefix_does_not_exist(self):
        original_key = 'key/full/path'

        class UnderlyingProxy(object):
            def get(self, key):
                if key == original_key:
                    return None
                if key == 'prefix/%s' % original_key:
                    return 'prefix'
                if key == 'prefix/optional_prefix/key/full/path':
                    return None

        proxy = PrefixedConfigurationProxy(UnderlyingProxy(), ['prefix'], ['optional_prefix'])
        assert proxy.get(original_key) == 'prefix'

