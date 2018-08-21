from pylib.hadoop.hdfs_util import get_size as size_on_hdfs, file_exists as exists_hdfs, directory_exists as dir_exists_hdfs
from pylib.aws.s3.inventory import get_size as size_on_s3, does_exist as exists_s3

import os

DEFAULT_BACKUP_BUCKET = 'similargroup-backup-retention'
DEFAULT_PREFIX = 'mrp'
SUCCESS_MARKER = '_SUCCESS'

# TODO there is infra in s3/data_checks that solves some this, given a connection and bucket objects
# it might be more efficient, but do we really want hold s3 constructs here? need to decide


def _s3_path(hdfs_path, s3_bucket=DEFAULT_BACKUP_BUCKET, prefix=DEFAULT_PREFIX):
    return '%s/%s%s' % (s3_bucket, prefix or '', hdfs_path)


class DataArtifact(object):
    def __init__(self, path, required_size=0, required_marker=True, bucket=DEFAULT_BACKUP_BUCKET, pref=DEFAULT_PREFIX):
        self.raw_path = path
        self.min_required_size = required_size
        self.check_marker = required_marker
        self.bucket = bucket
        self.prefix = pref

    def _hdfs_size(self):
        if dir_exists_hdfs(self.raw_path):
            return size_on_hdfs(self.raw_path)
        else:
            return None

    def _s3_size(self):
        return size_on_s3('s3://%s' % _s3_path(self.raw_path))

    @property
    def actual_size(self):
        hdfs_size = self._hdfs_size()
        if hdfs_size is not None:
            return hdfs_size
        else:
            return self._s3_size()

    def check_size(self):
        return self.actual_size >= self.required_size

    def assert_validity(self, msg='No data'):
        assert self.resolved_path is not None, msg  # this combines size check with marker validation

    @property
    def resolved_path(self):
        hdfs_size = self._hdfs_size()
        if hdfs_size is not None and hdfs_size > self.min_required_size and \
                (not self.check_marker or exists_hdfs(os.path.join(self.raw_path, SUCCESS_MARKER))):
            return self.raw_path
        else:
            s3_size = self._s3_size()
            if s3_size > self.min_required_size and (not self.check_marker or exists_s3(os.path.join('s3://%s' % _s3_path(self.raw_path), SUCCESS_MARKER))):
                return 's3a://%s' % _s3_path(self.raw_path)
            else:
                return None
