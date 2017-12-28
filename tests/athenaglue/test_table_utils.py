import pytest
import pprint
from pylib.athena_glue.table_utils import repair_table,create_new_version


class TestTableUtils:
    def test_repair_table(self):
        result = repair_table('iddoa','medicare_hospital_provider_csv')
        #print get_results()
        print result

    def test_create_new_version(self):
        result = create_new_version('iddoa','iddo_downloads','1','2')
        pprint.pprint(result)

