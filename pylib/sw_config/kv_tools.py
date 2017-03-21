try:
    import ujson as json
except ImportError:
    import json


class KeyValueTree(object):
    def __init__(self, kv_repr=None):
        if kv_repr is None:
            self._entries = 0
            self.root = dict()
        else:
            self.root = json.loads(kv_repr)
            self._entries = len([_ for (_, _) in self])

    def add_kv(self, key, value):
        key_parts = key.split('/')
        curr_node = self.root
        for kp in key_parts:
            next_node = curr_node.get(kp, None)
            if next_node is None:
                curr_node[kp] = dict()

            curr_node = curr_node[kp]

        if not curr_node.has_key('_value'):
            self._entries += 1
        curr_node['_value'] = value

    @staticmethod
    def _iter_node(paths, node):
        if '_value' in node:
            yield '/'.join(paths), node['_value']
        else:
            for sub_node, sub_tree in node.iteritems():
                for k1, v1 in KeyValueTree._iter_node(paths + [sub_node], sub_tree):
                    yield k1, v1

    def __iter__(self):
        return self._iter_node([], self.root)

    def __add__(self, other):
        ret = KeyValueTree()

        for self_k, self_v in self:
            ret.add_kv(self_k, self_v)
        for other_k, other_v in other:
            ret.add_kv(other_k, other_v)

        return ret

    def __len__(self):
        return self._entries


def kv_to_tree(kv, branch=None):
    """

    :param kv: key value proxy
    :param branch: subtree
    :return: KeyValueTree object mapping the key value store
    """

    ret = KeyValueTree()
    for key, value in kv.items(prefix=branch):
        ret.add_kv(key, value)

    return ret


def kv_diff(kv1, kv2, two_sided=False, branch=None, branch2=None):
    """
    calculates the differences between tow given key value stores

    :param kv1: key value proxy for first store
    :param kv2: key value proxy for second store
    :param two_sided: whether to also return kv2 - kv1, in addition to kv1 - kv2
    :param branch: branch to iterate on both key values. default is the the root
    :param branch2: option to override branch given, for kv2 alone
    :return: KeyValueTree representing the differences
    """

    ret = KeyValueTree()
    for key, val in kv1.items(prefix=branch):
        comparable = kv2.get_or_default(key)
        if val != comparable:
            ret.add_kv(key, {'source': val, 'target': comparable})

    if two_sided:
        for key, val in kv2.items(prefix=branch2 or branch):
            comparable = kv1.get_or_default(key)
            if val != comparable:
                ret.add_kv(key, {'source': comparable, 'target': val})

    return ret


def load_kv(kv, kv_repr):
    """
    loads key value tree represented by the argument to the key value proxy kv

    :param kv: target. key value proxy
    :param repr: (string) json representation of the key value chain to load
    :return: nothing
    """

    to_add = KeyValueTree(kv_repr)
    for key, value in to_add:
        kv.set(key, value)
