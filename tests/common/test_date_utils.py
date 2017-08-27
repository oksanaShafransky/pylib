from unittest import TestCase
from datetime import date
from common import date_utils


class TestDateUtils(TestCase):

    def test_get_years_range(self):
        output = date_utils.get_dates_range(date(year=2016, month=12, day=2), 3, 'years')
        expected = [date(year=2016, month=12, day=2), date(year=2015, month=12, day=2), date(year=2014, month=12, day=2)]
        assert output == expected
    
    def test_get_months_range(self):
        output = date_utils.get_dates_range(date(year=2016, month=2, day=2), 3, 'months')
        expected = [date(year=2016, month=2, day=2), date(year=2016, month=1, day=2), date(year=2015, month=12, day=2)]
        assert output == expected
    
    def test_get_weeks_range(self):
        output = date_utils.get_dates_range(date(year=2016, month=2, day=2), 3, 'weeks')
        expected = [date(year=2016, month=2, day=2), date(year=2016, month=1, day=26), date(year=2016, month=1, day=19)]
        assert output == expected
    
    def test_get_days_range(self):
        output = date_utils.get_dates_range(date(year=2016, month=2, day=2), 3)
        expected = [date(year=2016, month=2, day=2), date(year=2016, month=2, day=1), date(year=2016, month=1, day=31)]
        assert output == expected
