class PathTree(object):
    def __init__(self):
        self._entries = 0
        self.root = dict()

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
                for k1, v1 in PathTree._iter_node(paths + [sub_node], sub_tree):
                    yield k1, v1

    def __iter__(self):
        return self._iter_node([], self.root)

    def __add__(self, other):
        ret = PathTree()

        for self_k, self_v in self:
            ret.add_kv(self_k, self_v)
        for other_k, other_v in other:
            ret.add_kv(other_k, other_v)

        return ret

    def __len__(self):
        return self._entries


def kv_to_tree(kv, branch=None):
    ret = PathTree()
    for key, value in kv.items(prefix=branch):
        ret.add_kv(key, value)

    return ret


def kv_diff(kv1, kv2, two_sided=False, branch=None, branch2=None):
    ret = PathTree()
    for key, val in kv1.items(prefix=branch):
        comparable = kv2.get_or_default(key)
        if val != comparable:
            ret.add_kv(key, (val, comparable))

    if two_sided:
        for key, val in kv2.items(prefix=branch2 or branch):
            comparable = kv1.get_or_default(key)
            if val != comparable:
                ret.add_kv(key, (comparable, val))

    return ret
