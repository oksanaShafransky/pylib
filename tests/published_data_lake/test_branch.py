from __future__ import print_function
from pylib.published_data_lake.branch import GlueBranch
import pylib.published_data_lake.branch
# noinspection PyPackageRequirements
import pytest


class TestTableUtils(object):

    @pytest.fixture(scope="module")
    def cgc(self):
        return GlueBranch('iddoa', '1514732684')

    @pytest.fixture(scope="module")
    def branched_tabled(self):
        return 'iddo_downloads'

    def test_put_partition(self, monkeypatch):

        actual_commands = []

        # noinspection PyUnusedLocal
        def mock_athena_query(mock_self, query):
            actual_commands.append(query)
            if 'ADD PARTITION' in query:
                return {'state': 'FAILED', 'state_change_reason': 'Partition already exists'}
            else:
                return {'state': 'SUCCEEDED', 'state_change_reason': None}

        monkeypatch.setattr(GlueBranch, '_GlueBranch__athena_query', mock_athena_query)
        ans = self.cgc().put_partition(self.branched_tabled(), 'year=18/month=10/day=25')

        assert "ALTER TABLE iddo_downloads_1514732684 ADD PARTITION (year='18', month='10', day='25') location" in \
               actual_commands[0]
        assert "ALTER TABLE iddo_downloads_1514732684 PARTITION (year='18', month='10', day='25') set location" in \
               actual_commands[1]
        assert ans

    def test_fork_branch(self, monkeypatch):

        calls = []

        def mock_get_client():
            class MockClient:
                def __init__(self):
                    pass

                def get_table(*args, **kwargs):
                    calls.append({'foo': 'get_table', 'args': args, 'kwargs': kwargs})
                    return {u'Table': {u'StorageDescriptor': {
                        u'Location': u's3://sw-dag-published-v2/testmetric/1514732684',
                        u'PartitionKeys': [{u'Type': u'string', u'Name': u'year'},
                                           {u'Type': u'string', u'Name': u'month'},
                                           {u'Type': u'string', u'Name': u'day'}],
                        u'Name': u'test-metric_1514732684',
                    }}}

                def create_table(*args, **kwargs):
                    calls.append({'foo': 'create_table', 'args': args, 'kwargs': kwargs})

                def get_partitions(*args, **kwargs):
                    calls.append({'foo': 'get_partitions', 'args': args, 'kwargs': kwargs})
                    return {
                        u'Partitions': [{
                            u'StorageDescriptor': {
                                u'Location': u's3://sw-dag-published-v2/testmetric/1514732684',
                            },
                            u'TableName': u'testmetric_1514732684',
                            u'Values': [
                                u'18',
                                u'10',
                                u'25'],
                            u'DatabaseName': u'iddoa'}
                        ]}

                def batch_create_partition(*args, **kwargs):
                    calls.append({'foo': 'batch_create_partition', 'args': args, 'kwargs': kwargs})
                    return {u'Errors': [
                        {u'PartitionValues': [u'18', u'10', u'25'],
                         u'ErrorDetail': {u'ErrorCode': u'AlreadyExistsException',
                                          u'ErrorMessage': u'Partition already exists.'}}
                    ],
                        'ResponseMetadata':
                            {'HTTPStatusCode': 200}
                    }

            return MockClient()

        monkeypatch.setattr(GlueBranch, 'list_branchable_tables',
                            lambda _: ['iddo-ios-downloads'])

        monkeypatch.setattr(pylib.published_data_lake.branch, 'get_glue_client', mock_get_client)

        ans = self.cgc().fork_branch('testmetric')
        assert ans
        assert map(lambda x: x['foo'], calls) == ['get_table', 'create_table', 'get_partitions',
                                                  'batch_create_partition']
