import logging
import json

from pylib.hadoop.hdfs_util import get_size as size_on_hdfs, file_exists as exists_hdfs, directory_exists as dir_exists_hdfs
from pylib.aws.s3.inventory import get_size as size_on_s3, does_exist as exists_s3
from pylib.config.SnowflakeConfig import SnowflakeConfig
from itertools import ifilter

import os

SUCCESS_MARKER = '_SUCCESS'
DEFAULT_SUFFIX_FORMAT = '''year=%y/month=%m/day=%d'''

# TODO there is infra in s3/data_checks that solves some this, given a connection and bucket objects
# it might be more efficient, but do we really want hold s3 constructs here? need to decide

logger = logging.getLogger('data_artifact')


def human_size(raw_size):
    scale = 1024
    sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'HB']
    curr_size, curr_idx = float(raw_size), 0

    while curr_size >= scale:
        curr_size, curr_idx = curr_size / scale, curr_idx + 1

    return curr_size, sizes[curr_idx]


def get_s3_buckets(buckets=None):
    effective_buckets = buckets or SnowflakeConfig().get_service_name(service_name="da-s3-buckets")
    return json.loads(effective_buckets)


class RangedDataArtifact(object):

    def __init__(self, collection_path, dates, suffix_format=DEFAULT_SUFFIX_FORMAT, *args, **kwargs):
        self.collection_path = collection_path
        self.dates = dates
        self.suffix_format = suffix_format
        # Create list of dataartifacts
        self.ranged_data_artifact = [
            DataArtifact(os.path.join(self.collection_path, d.strftime(self.suffix_format)), *args, **kwargs)
            for d in self.dates
        ]

    def assert_input_validity(self, *reporters):
        for da in self.ranged_data_artifact:
            da.assert_input_validity(*reporters)

    def resolved_paths_string(self, item_delimiter=','):
        return item_delimiter.join([da.resolved_path for da in self.ranged_data_artifact])

    def resolved_paths_dates_string(self, date_format='%Y-%m-%d', item_delimiter=';', tuple_delimiter=','):
        tuples = [
            (self.ranged_data_artifact[i].resolved_path, self.dates[i].strftime(date_format))
            for i in range(len(self.dates))
        ]
        return item_delimiter.join([tuple_delimiter.join(tup) for tup in tuples])


class DataArtifact(object):
    def __init__(self, path, required_size=0, required_marker=True, bucket=None, pref=None, buckets=None):
        self.raw_path = path
        self.min_required_size = required_size
        self.check_marker = required_marker
        self.buckets = self._get_override_buckets(bucket, pref) or buckets

    def _get_override_buckets(self, override_bucket, override_prefix):
        class Bucket(object):
            def __init__(self, name, prefix=None):
                self.name = name
                self.prefix = prefix

        return json.dumps([Bucket(override_bucket, override_prefix).__dict__]) if override_bucket else None

    def _hdfs_size(self):
        if dir_exists_hdfs(self.raw_path):
            return size_on_hdfs(self.raw_path)
        else:
            return None

    def _s3_path(self, bucket, prefix=None):
        return '%s%s%s' % (bucket, prefix or '', self.raw_path)

    def _resolve_s3_size(self):
        path = self._resolve_s3_path_by_size()
        return size_on_s3('s3://%s' % path) if path else 0

    def _resolve_s3_path_by_size(self):
        def paths_by_buckets():
            bucks = get_s3_buckets(self.buckets)
            return [self._s3_path(b.get('name'), b.get('prefix')) for b in bucks]

        def filter_by_size(path):
            return size_on_s3('s3://%s' % path) > 0

        return next(ifilter(filter_by_size, paths_by_buckets()), None)

    @property
    def actual_size(self):
        hdfs_size = self._hdfs_size()
        if hdfs_size is not None:
            return hdfs_size
        else:
            return self._resolve_s3_size()

    def check_size(self):
        return self.actual_size >= self.min_required_size

    def _is_data_valid(self, direction, max_size, *reporters):
        hdfs_size = self._hdfs_size()
        check_marker_ok = True
        if hdfs_size is not None:
            logger.info('Checking that dir %s on hdfs is larger than %d...' % (self.raw_path, self.min_required_size))
            effective_size = hdfs_size
            if self.check_marker and not exists_hdfs(os.path.join(self.raw_path, SUCCESS_MARKER)):
                check_marker_ok = False
        else:
            effective_s3_path = self._resolve_s3_path_by_size()
            if effective_s3_path is None:
                logger.error('dir %s not found in hdfs or in s3' % self.raw_path)
                return False

            logger.info('Checking that dir %s on s3 is larger than %d...' % (effective_s3_path, self.min_required_size))
            effective_size = size_on_s3('s3://%s' % effective_s3_path)
            if self.check_marker and not exists_s3(os.path.join('s3://%s' % effective_s3_path, SUCCESS_MARKER)):
                check_marker_ok = False

        for reporter in reporters:
            # TODO decide how to treat the distinction of data found on hdfs/s3
            reporter.report_lineage('input', {self.raw_path: effective_size})

        resolved_path = self.resolved_path
        if not effective_size >= self.min_required_size:
            logger.error('%s data is not valid at %s. size is %s, required %s' % (direction, resolved_path, human_size(effective_size), human_size(self.min_required_size)))
            return False

        if not check_marker_ok:
            logger.error('no success marker was found at path %s' % resolved_path)
            return False

        if max_size is not None:
            if not effective_size >= self.min_required_size:
                logger.error('requested threshold too small for %s data  at %s. size is %s, required %s' % (direction, resolved_path, human_size(effective_size), human_size(self.min_required_size)))
                return False

        logger.info('Data validity checks passed for path %s' % resolved_path)
        return True

    def _assert_data_validity(self, direction, max_size, *reporters):
        assert self._is_data_valid(direction, max_size, *reporters)

    def is_input_valid(self, *reporters):
        return self._is_data_valid('input', None, *reporters)

    def assert_input_validity(self, *reporters):
        self._assert_data_validity('input', None, *reporters)

    STRICT_SIZE_THRESHOLD = 30

    def is_output_valid(self, is_strict=False, *reporters):
        return self._is_data_valid('output', self.min_required_size * DataArtifact.STRICT_SIZE_THRESHOLD if is_strict else None, *reporters)

    def assert_output_validity(self, is_strict=False, *reporters):
        self._assert_data_validity('output', self.min_required_size * DataArtifact.STRICT_SIZE_THRESHOLD if is_strict else None, *reporters)

    def is_local(self):
        return dir_exists_hdfs(self.raw_path)

    @property
    def resolved_path(self):
        if self.is_local():
            return self.raw_path
        else:
            path = self._resolve_s3_path_by_size()
            return None if not path else "s3a://%s" % path


if __name__ == '__main__':
    da = DataArtifact('path')

