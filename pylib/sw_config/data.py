from datetime import datetime, timedelta
from monthdelta import monthmod

__author__ = 'Felix'


class Artifact(object):
    class Mode:
        window = 0,
        monthly = 1

    def __init__(self, proxy, root_path, required_value='success', date_fmt='%Y-%m-%d', lookback=10, mode=Mode.window):
        self.proxy = proxy
        self.root = root_path
        self.req_val = required_value
        self.fmt = date_fmt
        self.lookback = lookback
        self.mode = mode

    def _accept_date(self, dt):
        now = datetime.now()
        if self.mode == Artifact.Mode.window:
            date_age = now - dt
            return date_age < timedelta(days=self.lookback)
        else:
            date_age = monthmod(dt, now)
            return date_age[0] < self.lookback

    @property
    def dates(self):
        dates = []
        for key in self.proxy.sub_keys(self.root) or []:
            potential_date = datetime.strptime(key, self.fmt)
            if not self._accept_date(potential_date):
                continue

            if self.proxy.get('%s/%s' % (self.root, key)) == self.req_val:
                dates.append(potential_date)

        return sorted(dates, reverse=True)


class Intersect(object):
    def __init__(self, *args):
        self.sub_artifacts = args

    @property
    def dates(self):
        return list(set.intersection(*[set(arg.dates) for arg in self.sub_artifacts]))

    @property
    def roots(self):
        return list(arg.root for arg in self.sub_artifacts)

    def deletes_in_roots(self, deletes):
        for delete in deletes:
            for root in self.roots:
                if root in delete:
                    return True
        return False
