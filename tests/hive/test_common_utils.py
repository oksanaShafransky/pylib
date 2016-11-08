import pytest
from pylib.hive.common import get_monthly_where


class TestHdfsUtil:
    def test_get_monthly_where(self):
        assert get_monthly_where(16, 12) == 'year=16 AND month=12'
        assert get_monthly_where(16, 12, 31) == 'year=16 AND month=12 AND day=31'
        assert get_monthly_where(16, 12, table_prefix='a') == 'a.year=16 AND a.month=12'
        assert get_monthly_where(16, 12, 31, 'a') == 'a.year=16 AND a.month=12 AND a.day=31'
