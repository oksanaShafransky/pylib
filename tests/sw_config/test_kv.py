from pylib.sw_config.kv_tools import list_kv
from pylib.sw_config.dict_change_simulator import DictProxy


class TestTools(object):

    def test_list(self):
        example_dict = {
            '/a': 12, '/b': 6, '/c': 19
        }
        proxy = DictProxy(**example_dict)
        assert example_dict == dict(list_kv(proxy))

    def test_subtree(self):
        example_dict = {
            '/a/b': 12, '/a/c': 6, '/b/b': 19
        }
        proxy = DictProxy(**example_dict)
        a_tree = list_kv(proxy, '/a')
        example_dict.pop('/b/b')
        assert example_dict == dict(a_tree)

