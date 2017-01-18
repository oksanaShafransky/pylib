from datetime import datetime
from monthdelta import monthmod

from kv import KeyValueProxy
from pylib.common.dependency import get_instance

__author__ = 'Felix'


def get_proxy():
    return get_instance(KeyValueProxy)


class Artifact(object):
    class Mode:
        window = 0,
        monthly = 1

    def __init__(self, root_path, required_value='success', date_fmt='%Y-%m-%d', lookback=10, mode=Mode.window):
        self.proxy = get_proxy()
        self.root = root_path
        self.req_val = required_value
        self.fmt = date_fmt
        self.lookback = lookback
        self.mode = mode

    @property
    def dates(self):
        potential_dates = []
        for key in self.proxy.sub_keys(self.root):
            if self.proxy.get('%s/%s' % (self.root, key)) == self.req_val:
                potential_dates.append(datetime.strptime(key, self.fmt))
        potential_dates = sorted(potential_dates, reverse=True)

        if not potential_dates:
            return []

        # keep only lookback days/months prior to first date
        dates = []
        delta = 0
        newer_date = potential_dates[0]

        for curr_date in potential_dates:
            overall_delta = monthmod(curr_date, newer_date)
            if self.mode == Artifact.Mode.window:
                delta += overall_delta[1].days
            else:
                delta += overall_delta[0].months

            if delta < self.lookback:
                dates += [curr_date]
                newer_date = curr_date
            else:
                break

        return dates


class Intersect(object):
    def __init__(self, *args):
        self.sub_artifacts = args

    @property
    def dates(self):
        return list(set.intersection(*[set(arg.dates) for arg in self.sub_artifacts]))
