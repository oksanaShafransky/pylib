from pylib.sw_config.dict_change_simulator import DictProxy


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

