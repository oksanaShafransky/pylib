import sys
import json
from dateutil.parser import parse


def parse_date_range(range_str):
    sep_idx = range_str.find(':')
    start_str = range_str[:sep_idx]
    end_str = range_str[sep_idx + 1:]

    if not start_str:
        start = None
    else:
        start = parse(start_str)

    if not end_str:
        end = None
    else:
        end = parse(end_str)

    return start, end


def parse_date_sequence(seq_str):
    ret = []

    ranges = seq_str.split(',')
    for range_str in ranges:
        ret += [parse_date_range(range_str)]

    return ret


def is_within(date, start, end):
    return (not start or date >= start) and (not end or date <= end)


def is_relevant(date, range_seq):
    for (start, end) in range_seq:
        if is_within(date, start, end):
            return True
    else:
        return False


class BlockedSourcesFetcher:

    blocked_sources_key_template = 'services/%s/source-blacklist/'

    def __init__(self, kv_proxy=None):
        if kv_proxy is not None:
            self.kv = kv_proxy
        else:
            from pylib.tasks.ptask_infra import TasksInfra
            self.kv = TasksInfra.kv()

    def get_blocked(self, module, check_date):
        module_key = BlockedSourcesFetcher.blocked_sources_key_template % module
        sources = []

        for source_path, block_conf in self.kv.items(module_key):
            source = source_path.lstrip(module_key).lstrip('/')
            date_seq = parse_date_sequence(block_conf)
            if is_relevant(check_date, date_seq):
                sources += [source]

        return sources

if __name__ == '__main__':

    module = sys.argv[1]
    check_date = parse(sys.argv[2])

    print ','.join(BlockedSourcesFetcher().get_blocked(module, check_date))







