from pylib.consistency.consistency_utils import ConsistencyTestInfra
from collections import namedtuple
from datetime import date
import pytest

from pylib.consistency.paths import ConsistencyPaths

#
# class TestInfra:
#
    # @pytest.fixture()
    # def ti(self):
    #     return namedtuple('ContextualizedTasksInfra', ['base_dir', 'date'])(
    #         base_dir='/base/dir',
    #         date=date(2015, 3, 22)
    #     )
    #
    # @pytest.fixture()
    # def countries(self):
    #     return [10, 20, 30, 40, 50]
    #
    # class TestGenModelPaths:
    #     def test_with_day_partition(self, countries):
    #         paths = ConsistencyTestInfra._gen_model_paths('/base/dir', 'name1', date(2012, 3, 5), True, countries)
    #         assert len(paths) == len(countries)
    #         example_path = '/base/dir/consistency/model/name1/year=12/month=03/day=05/country=%s' % countries[0]
    #         assert example_path in paths
    #
    #     def test_without_day_partition(self, countries):
    #         paths = ConsistencyTestInfra._gen_model_paths('/base/dir', 'name1', date(2012, 3, 5), False, countries)
    #         assert len(paths) == len(countries)
    #         example_path = '/base/dir/consistency/model/name1/year=12/month=03/country=%s' % countries[0]
    #         assert example_path in paths
    #
    # class TestGenResultPaths:
    #     def test_with_day_partition(self, countries):
    #         paths = ConsistencyTestInfra._gen_result_paths('/base/dir', 'name1', 'type1', date(2012, 3, 5), True, countries)
    #         assert len(paths) == len(countries)
    #         example_path = '/base/dir/consistency/output/name1/type=type1/year=12/month=03/day=05/country=%s' % countries[0]
    #         assert example_path in paths
    #
    #     def test_without_day_partition(self, countries):
    #         paths = ConsistencyTestInfra._gen_result_paths('/base/dir', 'name1', 'type1', date(2012, 3, 5), False, countries)
    #         assert len(paths) == len(countries)
    #         example_path = '/base/dir/consistency/output/name1/type=type1/year=12/month=03/country=%s' % countries[0]
    #         assert example_path in paths
    #
    # class TestGenTotalResultPaths:
    #     def test_with_day_partition(self):
    #         path = ConsistencyTestInfra._gen_total_result_path('/base/dir', 'name1', date(2012, 3, 5), True)
    #         assert path == '/base/dir/consistency/output/name1/type=total/year=12/month=03/day=05'
    #
    #     def test_without_day_partition(self):
    #         path = ConsistencyTestInfra._gen_total_result_path('/base/dir', 'name1', date(2012, 3, 5), False)
    #         assert path == '/base/dir/consistency/output/name1/type=total/year=12/month=03'
    #
    #
    # class TestGenInputDates:
    #
    #     @pytest.fixture
    #     def base_date(self):
    #         return date(2014, 3, 3)
    #
    #     def test_monthly_dates(self, base_date):
    #         dates = ConsistencyTestInfra._gen_input_dates(base_date, 'month')
    #         assert date(2014, 3, 1) in dates
    #         assert date(2014, 2, 1) in dates
    #         assert date(2014, 1, 1) in dates
    #         assert date(2013, 12, 1) in dates
    #         assert date(2013, 11, 1) in dates
    #
    #     def test_day_of_week_dates(self, base_date):
    #         dates = ConsistencyTestInfra._gen_input_dates(base_date, 'day_of_week')
    #         assert date(2014, 3, 3) in dates
    #         assert date(2014, 2, 24) in dates
    #         assert date(2014, 2, 17) in dates
    #         assert date(2014, 2, 10) in dates
    #         assert date(2014, 2, 3) in dates
    #
    #     def test_days_dates(self, base_date):
    #         dates = ConsistencyTestInfra._gen_input_dates(base_date, 'day')
    #         assert date(2014, 3, 3) in dates
    #         assert date(2014, 3, 2) in dates
    #         assert date(2014, 3, 1) in dates
    #         assert date(2014, 2, 28) in dates
    #         assert date(2014, 2, 27) in dates
