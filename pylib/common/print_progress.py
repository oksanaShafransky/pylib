
def print_progress(iterable, every=100, with_total=True, precent=False, total=None):
    total_suffix = ''
    if with_total:
        if total is None:
            iterable = list(iterable)
            total = len(iterable)
        total_suffix = '/%d' % total
        if precent:
            every = int(total * precent / 100)
    #
    for i, item in enumerate(iterable):
        if i % every == 0:
            print '%d%s' % (i+1, total_suffix)
        yield item