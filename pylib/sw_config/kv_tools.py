class PathTree(object):
    def __init__(self):
        self.root = dict()

    def add_kv(self, key, value):
        key_parts = key.split('/')
        curr_node = self.root
        for kp in key_parts:
            next_node = curr_node.get(kp, None)
            if next_node is None:
                curr_node[kp] = dict()

            curr_node = curr_node[kp]

        curr_node['_value'] = value

