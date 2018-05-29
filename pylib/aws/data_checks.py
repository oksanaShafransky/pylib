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


def validate_success(s3_conn, bucket_name, path):
    bucket = s3_conn.get_bucket(bucket_name)
    return len([f for f in bucket.list(_dirify(path) + '_SUCCESS')]) > 0


def s3_files_exist(s3_conn, bucket_name, path):
    bucket = s3_conn.get_bucket(bucket_name)
    return len(list(bucket.list(prefix=_dirify(path)))) > 0


def get_dates(bucket, path, is_ymd=True, check_success=False):
    sub_dirs = [ymd for ymd in sub_keys(bucket, path, levels=3) if ymd_pat.match(ymd)] \
        if is_ymd else \
        [ym for ym in sub_keys(bucket, _dirify(path), levels=2) if ym_pat.match(ym)]

    sub_dirs = [sd.replace('_$folder$', '') for sd in sub_dirs]
    if check_success:
        sub_dirs = [sd for sd in sub_dirs if validate_success(bucket, sd)]

    date_fmt = 'year=%y/month=%m/day=%d/' if is_ymd else 'year=%y/month=%m/'
    return [datetime.strptime(_dirify(sd)[len(_dirify(path)):], date_fmt) for sd in sub_dirs]


def get_s3_folder_size(s3_conn, bucket_name, path):
    '''Given a s3 folder name, retrieve the size of
    the sum of all files together. Returns the size in
    gigabytes and the number of objects.'''
    bucket = s3_conn.get_bucket(bucket_name)
    total_bytes = 0
    for key in bucket.list(prefix=_dirify(path)):
        total_bytes += key.size
    return total_bytes


def is_s3_folder_big_enough(s3_conn, bucket_name, path, min_size=0):
    '''Given a s3 folder name, retrieve True if the total of
    the folder is bigger then the min_size.
    Faster impelmantation than get_s3_folder_size'''
    bucket = s3_conn.get_bucket(bucket_name)
    total_bytes = 0
    for key in bucket.list(prefix=_dirify(path)):
        total_bytes += key.size
        if total_bytes > min_size:
            return True
    return False


if __name__ == '__main__':
    connector = boto.connect_s3()
    bucket = connector.get_bucket('similargroup-backup-retention')

    # example use
    print get_dates(bucket, 'mrp/similargroup/data/mobile-analytics/daily/parquet-visits', check_success=True)
