import pytest
import pprint
from pylib.athena_glue.table_utils import repair_checkpointed_table,create_new_checkpoint


class TestTableUtils:
    def test_repair_table(self):
        result = repair_checkpointed_table('iddoa','iddo_downloads')
        print result

    def test_create_new_version(self):
        result = create_new_checkpoint('iddoa','iddo_downloads','2')
        pprint.pprint(result)

