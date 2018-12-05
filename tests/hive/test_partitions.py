from pylib.hive.common import extract_sub_partitions


class TestPartitions(object):
    def test_found_partitions(self):
        base_partition = 'year=18/month=01'
        text = '''
        year=18/month=01/day=01
        year=18/month=01/day=02
        year=18/month=01/day=03
        year=18/month=01/day=04
        year=18/month=01/day=05
        year=18/month=01/day=06
        '''

        partitions = extract_sub_partitions(base_partition, text)
        assert len(partitions) == 6
        assert 'year=18/month=01/day=01' in partitions
        assert 'year=18/month=01/day=04' in partitions
