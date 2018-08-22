import boto
from StringIO import StringIO

from pylib.aws.s3 import parse_s3_url


class S3WriterWrapper:
    def __init__(self, s3_key, initial_content=''):
        self._output = s3_key
        self._stream = StringIO()
        self._stream.write(initial_content)

    def write(self, what):
        self._stream.write(what)

    def _flush(self):
        self._output.set_contents_from_string(self._stream.getvalue())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._flush()

    def __del__(self):
        self._flush()


def open(s3_url, mode='r'):
    s3_conn = boto.connect_s3()
    bucketname, filename = parse_s3_url(s3_url)
    bucket = s3_conn.get_bucket(bucketname)
    s3_file = bucket.get_key(filename)
    if s3_file is None:
        s3_file = bucket.new_key(filename)

    if mode == 'r':
        return s3_file
    elif mode == 'w':
        return S3WriterWrapper(s3_file)
    elif mode == 'a':
        return S3WriterWrapper(s3_file, s3_file.get_contents_as_string())
    else:
        raise AttributeError('unsupported open mode: %s' % mode)
