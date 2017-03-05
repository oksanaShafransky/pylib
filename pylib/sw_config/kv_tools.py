

# DFS generator on a kv proxy. objects are yielded as (key, value) tuples
def list_kv(kv, start_node=''):
    next_keys = [start_node]
    while len(next_keys) > 0:
        nk = next_keys.pop(0)
        try_val = kv.get(nk)
        if try_val is not None:
            yield nk, try_val
        else:
            for sk in kv.sub_keys(nk):
                next_keys.insert(0, '%s/%s' % (nk, sk))
