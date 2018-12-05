from pylib.hive.common import extract_partitions_by_prefix


class TestPartitions(object):
    def test_found_partitions(self):
        prefix = 'year=18/month=01'
        text = '''
        year=18/month=01/day=01
        year=18/month=01/day=02
        year=18/month=01/day=03
        year=18/month=01/day=04
        year=18/month=01/day=05
        year=18/month=01/day=06
        '''

        partitions = extract_partitions_by_prefix(prefix, text)
        assert len(partitions) == 6
        assert 'year=18/month=01/day=01' in partitions
        assert 'year=18/month=01/day=04' in partitions

    def test_partition_not_found(self):
        prefix = 'year=18/month=02'
        text = '''
        year=18/month=01/day=01
        year=18/month=01/day=02
        year=18/month=01/day=03
        year=18/month=01/day=04
        year=18/month=01/day=05
        year=18/month=01/day=06
        '''

        partitions = extract_partitions_by_prefix(prefix, text)
        assert len(partitions) == 0

    def test_ignore_exact_match(self):
        prefix = 'year=18/month=01'
        text = '''
        year=18/month=01
        year=18/month=01/day=02
        year=18/month=01/day=03
        year=18/month=01/day=04
        year=18/month=01/day=05
        year=18/month=01/day=06
        '''

        partitions = extract_partitions_by_prefix(prefix, text)
        assert len(partitions) == 5
        assert 'year=18/month=01' not in partitions
        assert 'year=18/month=01/day=02' in partitions
        assert 'year=18/month=01/day=04' in partitions

    def test_ignore_exact_match2(self):
        prefix = 'year=18/month=12/day=01'
        text = '''
year=18/month=11/day=29
year=18/month=11/day=30
year=18/month=12/day=01
year=18/month=12/day=02
        '''

        partitions = extract_partitions_by_prefix(prefix, text)
        assert len(partitions) == 0
        assert 'year=18/month=12/day=01' not in partitions





