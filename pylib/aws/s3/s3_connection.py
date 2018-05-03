import boto
from boto.s3.connection import S3Connection


def get_s3_connection(conf_file_path='/etc/aws-conf/.s3cfg', profile='default', access=None, secret=None):
    if access is None or secret is None:
        config = boto.pyami.config.Config(path=conf_file_path)
        access = config.get(profile, 'access_key')
        secret = config.get(profile, 'secret_key')
    return S3Connection(access, secret)