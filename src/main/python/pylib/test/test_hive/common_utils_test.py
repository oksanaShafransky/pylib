from unittest import TestCase
from pylib.hive.common import get_monthly_where


class HdfsUtilTest(TestCase):
    def test_get_monthly_where(self):
        self.assertEquals('year=16 AND month=12', get_monthly_where(16, 12))
        self.assertEquals('year=16 AND month=12 AND day=31', get_monthly_where(16, 12, 31))
        self.assertEquals('a.year=16 AND a.month=12', get_monthly_where(16, 12, table_prefix='a'))
        self.assertEquals('a.year=16 AND a.month=12 AND a.day=31', get_monthly_where(16, 12, 31, 'a'))
