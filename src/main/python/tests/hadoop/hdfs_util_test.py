from unittest import TestCase

from pylib.hadoop.hdfs_util import extract_hive_partition_values


class HdfsUtilTest(TestCase):
    def test_get_hive_partition_values_positive(self):
        self.assertEquals(['6'],
                          extract_hive_partition_values(
                              ['/user/hive/top_sites_by_device_source/type=monthly/year=16/month=3/source=6/000000_0'],
                              'source'))
        self.assertEquals(['7'],
                          extract_hive_partition_values(
                              ['/user/hive/top_sites_by_device_source/type=monthly/year=16/month=3/source=7/000000_0'],
                              'source'))
        self.assertEquals(['6', '7'],
                          extract_hive_partition_values(
                              ['/user/hive/top_sites_by_device_source/type=monthly/year=16/month=3/source=7/000000_0',
                               '/user/hive/top_sites_by_device_source/type=monthly/year=16/month=3/source=7',
                               '/user/hive/top_sites_by_device_source/type=monthly/year=16/month=3/source=6/000000_0',
                               '/user/hive/top_sites_by_device_source/type=monthly/year=16/month=3/source=6'],
                              'source'))

    def test_get_hive_partition_values_negative(self):
        self.assertRaises(AssertionError, extract_hive_partition_values, 'source=6/000000_0', 'source')
        self.assertRaises(AssertionError, extract_hive_partition_values, ['source=/000000_0'], 'source')
