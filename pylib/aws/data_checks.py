import boto
import re
from datetime import datetime


ym_pat = re.compile('.*year=\d{2}/month=\d{2}')
ymd_pat = re.compile('.*year=\d{2}/month=\d{2}/day=\d{2}')


def _dirify(dir):
    return dir if dir.endswith('/') else dir + '/'


def sub_keys(bucket, path, levels=1, delim='/'):
    ret = set()
    ret.add(path)
    curr_level = 0

    while curr_level < levels:
        next_level = set()
        for dir in ret:
            for sub_dir in [f.name[len(_dirify(dir)):] for f in bucket.list(prefix=_dirify(dir), delimiter=delim)]:
                next_level.add(_dirify(dir) + sub_dir)

        ret = next_level
        curr_level += 1

    return ret


def validate_success(path):
    print 'checking success for path ' + _dirify(path)
    return len([f for f in bucket.list(_dirify(path) + '_SUCCESS')]) > 0


def get_dates(bucket, path, is_ymd=True, check_success=False):
    sub_dirs = [ymd for ymd in sub_keys(bucket, path, levels=3) if ymd_pat.match(ymd)] \
        if is_ymd else \
        [ym for ym in sub_keys(bucket, _dirify(path), levels=2) if ym_pat.match(ym)]

    sub_dirs = [sd.replace('_$folder$', '') for sd in sub_dirs]
    if check_success:
        sub_dirs = [sd for sd in sub_dirs if validate_success(sd)]

    date_fmt = 'year=%y/month=%m/day=%d/' if is_ymd else 'year=%y/month=%m/'
    return [datetime.strptime(_dirify(sd)[len(_dirify(path)):], date_fmt) for sd in sub_dirs]


if __name__ == '__main__':

    connector = boto.connect_s3()
    bucket = connector.get_bucket('similargroup-backup-retention')

    # example use
    print get_dates(bucket, 'mrp/similargroup/data/mobile-analytics/daily/parquet-visits', check_success=True)




