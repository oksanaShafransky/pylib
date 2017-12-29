from __future__ import print_function
import pprint
from pylib.athena_glue.table_utils import repair_table, create_checkpoint


class TestTableUtils:
    def __init__(self):
        pass

    def test_repair_table(self):
        result = repair_table('iddoa', 'iddo_downloads')
        print(result)

    def test_create_new_version(self):
        result = create_checkpoint('iddoa', 'iddo_downloads', '2')
        pprint.pprint(result)
