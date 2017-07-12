from pylib.consistency.consistency_utils import ConsistencyTestInfra
from collections import namedtuple
from datetime import date
import pytest



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
        def test_with_day_partition(self, countries):
            paths = ConsistencyTestInfra.gen_model_paths('/base/dir', 'name1', date(2012, 3, 5), True, countries)
            assert len(paths) == len(countries)
            example_path = '/base/dir/consistency/model/name1/country=%s/year=12/month=03/day=05' % countries[0]
            assert example_path in paths

        def test_without_day_partition(self, countries):
            paths = ConsistencyTestInfra.gen_model_paths('/base/dir', 'name1', date(2012, 3, 5), False, countries)
            assert len(paths) == len(countries)
            example_path = '/base/dir/consistency/model/name1/country=%s/year=12/month=03/day=01' % countries[0]
            assert example_path in paths

    class TestGenResultPaths:
        def test_with_day_partition(self, countries):
            paths = ConsistencyTestInfra.gen_result_paths('/base/dir', 'name1', date(2012, 3, 5), True, countries)
            assert len(paths) == len(countries)
            example_path = '/base/dir/consistency/output/name1/bycountry/country=%s/year=12/month=03/day=05' % countries[0]
            assert example_path in paths

        def test_without_day_partition(self, countries):
            paths = ConsistencyTestInfra.gen_result_paths('/base/dir', 'name1', date(2012, 3, 5), False, countries)
            assert len(paths) == len(countries)
            example_path = '/base/dir/consistency/output/name1/bycountry/country=%s/year=12/month=03' % countries[0]
            assert example_path in paths
