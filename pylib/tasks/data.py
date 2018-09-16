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


def human_size(raw_size):
    scale = 1024
    sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'HB']
    curr_size, curr_idx = float(raw_size), 0

    while curr_size >= scale:
        curr_size, curr_idx = curr_size / scale, curr_idx + 1

    return curr_size, sizes[curr_idx]


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

    def _assert_data_validity(self, direction, max_size, *reporters):
        hdfs_size = self._hdfs_size()
        check_marker_ok = True
        if hdfs_size is not None:
            effective_size = hdfs_size
            if self.check_marker and not exists_hdfs(os.path.join(self.raw_path, SUCCESS_MARKER)):
                check_marker_ok = False
        else:
            effective_size = self._s3_size()
            if self.check_marker and not exists_s3(os.path.join('s3://%s' % _s3_path(self.raw_path), SUCCESS_MARKER)):
                check_marker_ok = False

        for reporter in reporters:
            # TODO decide how to treat the distinction of data found on hdfs/s3
            reporter.report_lineage('input', {self.raw_path: effective_size})

        assert effective_size >= self.min_required_size, '%s data is not valid at %s. size is %s, required %s' % (direction, self.raw_path, human_size(effective_size), human_size(self.min_required_size))
        assert check_marker_ok, 'no success marker at %s' % self.resolved_path
        if max_size is not None:
            assert effective_size >= self.min_required_size, 'requested threshold too small for %s data  at %s. size is %s, required %s' % (direction, self.resolved_path, human_size(effective_size), human_size(self.min_required_size))

    def assert_input_validity(self, *reporters):
        self._assert_data_validity('input', None, *reporters)

    STRICT_SIZE_THRESHOLD = 30
    def assert_output_validity(self, is_strict=False, *reporters):
        self._assert_data_validity('outupt', self.min_required_size * DataArtifact.STRICT_SIZE_THRESHOLD if is_strict else None, *reporters)

    @property
    def resolved_path(self):
        hdfs_size = self._hdfs_size()
        if hdfs_size is not None:
            return self.raw_path
        else:
            s3_size = self._s3_size()
            if s3_size > 0:
            #if s3_size > self.min_required_size and (not self.check_marker or exists_s3(os.path.join('s3://%s' % _s3_path(self.raw_path), SUCCESS_MARKER))):
                return 's3a://%s' % _s3_path(self.raw_path)
            else:
                return None
