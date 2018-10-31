from pylib.consistency.consistency_types import ConsistencyDateType, ConsistencyOutputType

from pylib.consistency.consistency_utils import ConsistencyTestInfra
from collections import namedtuple
from datetime import date
import pytest

from pylib.consistency.paths import ConsistencyPaths


class TestInfra:

    @pytest.fixture()
    def ti(self):
        return namedtuple('ContextualizedTasksInfra', ['base_dir', 'date'])(
            base_dir='/base/dir',
            date=date(2015, 3, 22)
        )

    @pytest.fixture()
    def countries(self):
        return [10, 20, 30, 40, 50]


    class TestGenModelPaths:
        def test_base_path_with_day_partition(self):
            path = ConsistencyPaths.gen_base_model_path('/base/dir', 'name1', date(2012, 3, 5), ConsistencyDateType.Day)
            required_path = '/base/dir/consistency/name1/model/year=12/month=03/day=05'
            assert required_path == path

        def test_base_path_without_day_partition(self):
            path = ConsistencyPaths.gen_base_model_path('/base/dir', 'name1', date(2012, 3, 5), ConsistencyDateType.Month)
            required_path = '/base/dir/consistency/name1/model/year=12/month=03'
            assert required_path == path

        def test_model_country_path(self):
            path = ConsistencyPaths.gen_model_country_path('/base/dir', 'name1', date(2012, 3, 5), 111, ConsistencyDateType.Day)
            required_path = '/base/dir/consistency/name1/model/year=12/month=03/day=05/type=countries/country=111'
            assert required_path == path

        def test_model_benchmark_path(self):
            path = ConsistencyPaths.gen_model_benchmark_path('/base/dir', 'name1', date(2012, 3, 5), ConsistencyDateType.Day)
            required_path = '/base/dir/consistency/name1/model/year=12/month=03/day=05/type=benchmark'
            assert required_path == path

        def test_model_all_countries_path(self, countries):
            paths = ConsistencyPaths.gen_all_model_paths('/base/dir', 'name1', date(2012, 3, 5), countries, ConsistencyDateType.Day)
            required_paths = ['/base/dir/consistency/name1/model/year=12/month=03/day=05/type=countries/country=%s' % country for country in countries]
            for rp in required_paths:
                assert rp in paths


    class TestGenTotalResultPaths:
        def test_base_output_path_with_day_partition(self):
            path = ConsistencyPaths.gen_base_output_path('/base/dir', 'name1', date(2012, 3, 5), ConsistencyOutputType.Total, ConsistencyDateType.Day)
            required_path = '/base/dir/consistency/name1/output/year=12/month=03/day=05/type=total'
            assert path == required_path

        def test_base_output_path_without_day_partition(self):
            path = ConsistencyPaths.gen_base_output_path('/base/dir', 'name1', date(2012, 3, 5), ConsistencyOutputType.Model, ConsistencyDateType.Month)
            required_path = '/base/dir/consistency/name1/output/year=12/month=03/type=model'
            assert path == required_path

        def test_country_output_path(self):
            path = ConsistencyPaths.gen_output_path('/base/dir', 'name1', date(2012, 3, 5), ConsistencyOutputType.Countries, ConsistencyDateType.Day, country=111)
            required_path = '/base/dir/consistency/name1/output/year=12/month=03/day=05/type=countries/country=111'
            assert path == required_path

        def test_all_output_path(self, countries):
            countries_paths = ['/base/dir/consistency/name1/output/year=12/month=03/day=05/type=countries/country=%s' % country for country in countries]
            model_path = ['/base/dir/consistency/name1/output/year=12/month=03/day=05/type=model/country=%s' % country for country in countries]
            total_path = '/base/dir/consistency/name1/output/year=12/month=03/day=05/type=total'
            all_paths = ConsistencyPaths.gen_all_output_paths('/base/dir', 'name1', date(2012, 3, 5), ConsistencyDateType.Day, countries)

            for cp in countries_paths:
                assert cp in all_paths

            for mp in model_path:
                assert mp in all_paths

            assert total_path in all_paths

    class TestGenInputPaths:

        def test_daily_paths(self):
            expected_paths = [
                '/base/dir/my/input/path/year=18/month=10/day=10',
                '/base/dir/my/input/path/year=18/month=10/day=09',
                '/base/dir/my/input/path/year=18/month=10/day=08',
                '/base/dir/my/input/path/year=18/month=10/day=07',
                '/base/dir/my/input/path/year=18/month=10/day=06'
            ]
            input_paths = ConsistencyPaths.gen_input_paths('/base/dir', 'my/input/path', date(2018, 10, 10), ConsistencyDateType.Day)

            for ep in expected_paths:
                assert ep in input_paths

        def test_day_of_week_paths(self):
            expected_paths = [
                '/base/dir/my/input/path/year=18/month=10/day=10',
                '/base/dir/my/input/path/year=18/month=10/day=03',
                '/base/dir/my/input/path/year=18/month=09/day=26',
                '/base/dir/my/input/path/year=18/month=09/day=19',
                '/base/dir/my/input/path/year=18/month=09/day=12'
            ]
            input_paths = ConsistencyPaths.gen_input_paths('/base/dir', 'my/input/path', date(2018, 10, 10), ConsistencyDateType.DayOfWeek)

            for ep in expected_paths:
                assert ep in input_paths

        def test_monthly_paths(self):
            expected_paths = [
                '/base/dir/my/input/path/year=18/month=10',
                '/base/dir/my/input/path/year=18/month=09',
                '/base/dir/my/input/path/year=18/month=08',
                '/base/dir/my/input/path/year=18/month=07',
                '/base/dir/my/input/path/year=18/month=06'
            ]
            input_paths = ConsistencyPaths.gen_input_paths('/base/dir', 'my/input/path', date(2018, 10, 10), ConsistencyDateType.Month)

            for ep in expected_paths:
                assert ep in input_paths

