import logging
import abc
from enum import Enum
import os
from pylib.hadoop.hdfs_util import get_size as size_on_hdfs, file_exists as exists_hdfs, directory_exists as dir_exists_hdfs, create_client
from pylib.aws.s3.inventory import get_size as size_on_s3, does_exist as exists_s3
import enum


logger = logging.getLogger('data_artifact')

SUCCESS_MARKER = '_SUCCESS'

def human_size(raw_size):
    scale = 1024
    sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'HB']
    curr_size, curr_idx = float(raw_size), 0

    while curr_size >= scale:
        curr_size, curr_idx = curr_size / scale, curr_idx + 1

    return str(curr_size) + str(sizes[curr_idx])

class DatasourceTypes(Enum):
   S3 = "s3"
   HDFS = "hdfs"


def create_datasource_input_dict(type, name, prefix):
    if not isinstance(type, DatasourceTypes):
        raise Exception("DataSource Error - DataSourcesTypes type was expected")

    return {'type': type.value,
            'name': name,
            'pref': prefix
    }


class DataSource(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, collection, prefix, required_size, required_marker):
        self.collection = collection
        self.prefix = prefix
        self.required_size = required_size
        self.required_marker = required_marker
        self.full_uri = None
        self.is_exist = None
        self.is_marker_validated = None
        self.is_size_validated = None
        self.effective_size = None


    @abc.abstractmethod
    def log_success(self):
        """Method documentation"""
        return

    @abc.abstractmethod
    def log_fail_to_find(self):
        """Method documentation"""
        return

    @abc.abstractmethod
    def assert_size(self):
        """Method documentation"""
        return

    @abc.abstractmethod
    def assert_marker(self):
        """Method documentation"""
        return

    @abc.abstractmethod
    def is_dir_exist(self):
        """Method documentation"""
        return

    @abc.abstractmethod
    def resolved_path(self):
        """Method documentation"""
        return

    @abc.abstractmethod
    def __repr__(self):
        """Method documentation"""
        return


class S3DataSource(DataSource):

    def __init__(self, collection, required_size, required_marker, bucket_name, prefix):
        super(S3DataSource, self).__init__(collection, prefix, required_size, required_marker)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.prefixed_collection = "%s%s" % (self.prefix, self.collection)
        self.full_uri = "s3a://%s%s" % (self.bucket_name, self.prefixed_collection)

    def log_success(self):
        logger.info(
            "DataArtifact S3(bucket:%s) - Location is located and validated. Collection: %s , Actual size: %s, Marker: %s" %
            (self.bucket_name, self.prefixed_collection, self.effective_size, self.required_marker))

    def assert_size(self):
        if not self.is_exist:
            logger.error('Validate size called when dir is not exist %s on s3 bucket: %s' % (self.prefixed_collection, self.bucket_name))
            raise Exception("DataArtifact Failure")

        logger.info(
            'Checking that dir %s on s3 is larger than %d...' % (self.prefixed_collection, self.required_size))
        assert self.effective_size >= self.required_size,\
            'Size test failed on %s required size: %d actual size: %d' % (self.prefixed_collection, self.required_size, self.effective_size)

        logger.info("Size is valid: %s" % human_size(self.effective_size))
        self.is_size_validated = True

    def assert_marker(self):
        if not self.is_exist:
            logger.error('Validate marker called when dir is not exist %s on s3 bucket: %s' % (self.prefixed_collection, self.bucket_name))
            raise Exception("DataArtifact Failure")

        logger.info('Checking if marker required or exist %s on s3 bucket: %s' % (
        self.prefixed_collection, self.bucket_name))
        self.is_marker_validated = exists_s3(os.path.join(self.full_uri, SUCCESS_MARKER))
        assert not self.required_marker or self.is_marker_validated, \
            'No success marker found in %s on s3 bucket %s' % (self.prefixed_collection, self.bucket_name)
        logger.info("Marker is valid, required_marker %s" % str(self.required_marker))
        self.is_marker_validated = True

    def is_dir_exist(self):
        #We already checked
        if self.is_exist:
            return True
        self.effective_size = size_on_s3(self.full_uri)
        logger.info('Checking if collection: %s exists on S3 bucket: %s' % (self.prefixed_collection, self.bucket_name))
        if self.effective_size != 0:
            self.is_exist = True
            return True

        return False

    def log_fail_to_find(self):
        logger.info("DataArtifact s3(bucket: %s) - Couldn't find %s collection" % (self.bucket_name, self.prefixed_collection))


    def resolved_path(self):
        if self.is_exist and self.is_marker_validated and self.is_size_validated:
            return self.full_uri
        else:
            raise Exception("DataArtifact Failure chosen datasource doesn't have valid path")

    def __repr__(self):
        return 'S3 Datasource reading from %s bucket with %s prefix(could be empty)' % (self.bucket_name, self.prefix)



class HDFSDataSource(DataSource):

    def __init__(self, collection, required_size, required_marker, name, prefix):
        super(HDFSDataSource, self).__init__(collection, prefix, required_size, required_marker)
        self.name = name
        self.prefixed_collection = "%s%s" % (self.prefix, self.collection)
        self.full_uri = "hdfs://%s%s" % (self.name, self.prefixed_collection)
        self.hdfs_client = create_client(self.name)


    def _check_hdfs_size(self):
        if dir_exists_hdfs(self.prefixed_collection, hdfs_client=self.hdfs_client):
            return size_on_hdfs(self.prefixed_collection, hdfs_client=self.hdfs_client)
        else:
            return None

    def log_success(self):
        logger.info(
            "DataSource HDFS(%s) - Location is located and validated. Collection: %s , Actual size: %s, Marker: %s, Full Uri: %s" %
            (self.name, self.prefixed_collection, self.effective_size, self.required_marker, self.full_uri))

    def log_fail_to_find(self):
        logger.info("DataSource HDFS(%s) - Couldn't find %s collection" % (self.name, self.prefixed_collection))


    def is_dir_exist(self):
        #We already checked
        if self.is_exist:
            return True
        self.effective_size = self._check_hdfs_size()
        logger.info('Checking if collection: %s exists on HDFS %s' % (self.prefixed_collection, self.name))
        if self.effective_size is not None:
            self.is_exist = True
            return True

        return False

    def assert_marker(self):
        if not self.is_exist:
            logger.error('Validate marker called when dir is not exist %s on hdfs: %s' % (self.prefixed_collection, self.name))
            raise Exception("DataArtifact Failure")

        logger.info('Checking if marker required or exist %s on hdfs: %s' % (self.prefixed_collection, self.name))
        is_marker_exist_in_hdfs = exists_hdfs(os.path.join(self.prefixed_collection, SUCCESS_MARKER),
                                                   hdfs_client=self.hdfs_client)

        assert not self.required_marker or is_marker_exist_in_hdfs, \
            'No success marker found in %s on HDFS %s' % (self.prefixed_collection, self.name)

        logger.info("Marker is valid, required_marker %s" % str(self.required_marker))
        self.is_marker_validated = True

    def assert_size(self):
        if not self.is_exist:
            logger.error('Validate size called when dir is not exist %s on hdfs: %s' % (self.prefixed_collection, self.name))
            raise Exception("DataArtifact Failure")

        logger.info(
            'Checking that dir %s on hdfs is larger than %d...' % (self.prefixed_collection, self.required_size))
        assert self.effective_size >= self.required_size, 'Size test failed on %s required size: %d actual size: %d'% (self.prefixed_collection, self.required_size, self.effective_size)

        logger.info("Size is valid: %s" % human_size(self.effective_size))
        self.is_size_validated = True


    def resolved_path(self):
        if self.is_exist and self.is_marker_validated and self.is_size_validated:
            return self.full_uri
        else:
            raise Exception("DataArtifact Failure chosen datasource doesn't have valid path")

    def __repr__(self):
        return 'HDFS Datasource reading from %s namenode with %s prefix(could be empty)' % (self.name, self.prefix)



