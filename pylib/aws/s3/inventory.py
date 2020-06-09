import boto
from pylib.aws.s3 import parse_s3_url


def get_size(s3_url):
    s3_conn = boto.connect_s3()
    bucketname, path = parse_s3_url(s3_url)
    # validate=False because there are some cases when we do not have head permission but read permissions
    # for specific paths (remote s3).. if bucket doesnt exist the list method will fail
    bucket = s3_conn.get_bucket(bucketname, validate=False)

    return sum([f.size for f in bucket.list(path)])


def does_exist(s3_url):
    s3_conn = boto.connect_s3()
    bucketname, filename = parse_s3_url(s3_url)
    bucket = s3_conn.get_bucket(bucketname, validate=False)

    return bucket.get_key(filename) is not None
