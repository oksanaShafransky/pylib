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
