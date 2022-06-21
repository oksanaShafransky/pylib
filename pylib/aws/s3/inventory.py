import boto3
import boto
from pylib.aws.s3 import parse_s3_url


def get_size(s3_url):
    # parse the url to get the bucket and path
    bucketname, path = parse_s3_url(s3_url)

    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")

    operation_parameters = {'Bucket': bucketname,
                            'Prefix': path}
    pages = paginator.paginate(**operation_parameters)

    # Go over all the pages from the response and sum them up
    path_size = 0
    for page in pages:
        if "Contents" in page:
            for key in page["Contents"]:
                path_size += key['Size']

    return path_size


def does_exist(s3_url):
    s3_conn = boto.connect_s3()
    bucketname, filename = parse_s3_url(s3_url)
    bucket = s3_conn.get_bucket(bucketname, validate=False)

    return bucket.get_key(filename) is not None
