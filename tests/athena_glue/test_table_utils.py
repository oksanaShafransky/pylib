from __future__ import print_function
import pprint
from pylib.athena_glue.table_utils import GlueBranch
import pytest


class TestTableUtils(object):

    @pytest.fixture(scope="module")
    def cgc(self):
        return GlueBranch('iddoa', '1514732684')

    @pytest.fixture(scope="module")
    def branched_tabled(self):
        return 'iddo_downloads'

    def test_put_partition(self):
        ans = self.cgc().put_partition(self.branched_tabled(),'year=18/month=10/day=25')
        assert ans==True

    def test_list_branchable_tables(self):
        ans = self.cgc().list_branchable_tables()
        print(ans)

    def test_fork_branch(self):
        ans = self.cgc().fork_branch('test123')
        print(ans)

