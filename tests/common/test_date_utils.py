from unittest import TestCase
from datetime import date
from pylib.common import date_utils


class TestDateUtils(TestCase):

    def test_get_years_range(self):
        output = date_utils.get_dates_range(date(year=2016, month=12, day=2), 3, 'years')
        expected = [date(year=2014, month=12, day=2), date(year=2015, month=12, day=2), date(year=2016, month=12, day=2)]
        assert output == expected
    
    def test_get_months_range(self):
        output = date_utils.get_dates_range(date(year=2016, month=2, day=2), 3, 'months')
        expected = [date(year=2015, month=12, day=2), date(year=2016, month=1, day=2), date(year=2016, month=2, day=2)]
        assert output == expected
    
    def test_get_weeks_range(self):
        output = date_utils.get_dates_range(date(year=2016, month=2, day=2), 3, 'weeks')
        expected = [date(year=2016, month=1, day=19), date(year=2016, month=1, day=26), date(year=2016, month=2, day=2)]
        assert output == expected
    
    def test_get_days_range(self):
        output = date_utils.get_dates_range(date(year=2016, month=2, day=2), 3)
        expected = [date(year=2016, month=1, day=31),  date(year=2016, month=2, day=1), date(year=2016, month=2, day=2)]
        assert output == expected

    def test_get_months_range_offset(self):
        output = date_utils.get_dates_range(date(year=2017, month=6, day=1), 3, 'months', date_offset=2)
        expected = [date(year=2017, month=2, day=1), date(year=2017, month=3, day=1), date(year=2017, month=4, day=1)]
        assert output == expected

    def test_get_weeks_range_step_size_2(self):
        output = date_utils.get_dates_range(date(year=2016, month=2, day=2), 3, 'weeks', step_size=2)
        expected = [date(year=2016, month=1, day=5), date(year=2016, month=1, day=19), date(year=2016, month=2, day=2)]
        assert output == expected

    def test_get_last_days_of_months(self):
        output = date_utils.get_dates_range(date(year=2016, month=12, day=31), 12, 'months')
        expected = [date(year=2016, month=1, day=31), date(year=2016, month=2, day=29),
                    date(year=2016, month=3, day=31), date(year=2016, month=4, day=30),
                    date(year=2016, month=5, day=31), date(year=2016, month=6, day=30),
                    date(year=2016, month=7, day=31), date(year=2016, month=8, day=31),
                    date(year=2016, month=9, day=30), date(year=2016, month=10, day=31),
                    date(year=2016, month=11, day=30), date(year=2016, month=12, day=31)]
        assert output == expected
