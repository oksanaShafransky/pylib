import pytest
from pylib.athena_glue.table_utils import repair_table


class TestTableUtils:
    def test_repair_table(self):
        result = repair_table('iddoa','medicare_hospital_provider_csv')
        #print get_results()
        print result
