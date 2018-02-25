from __future__ import print_function
from pylib.published_data_lake.branch import GlueBranch, BranchableTable
import pylib.published_data_lake.branch
# noinspection PyPackageRequirements
import pytest


class TestTableUtils(object):

    db = 'sw_published_mobile'

    @pytest.fixture(scope="module")
    def glue_branch(self):
        return GlueBranch(name='1514732684')

    @pytest.fixture(scope="module")
    def branched_table(self):
        return BranchableTable(name='metrica', db=TestTableUtils.db)

    def test_put_partition(self, monkeypatch, glue_branch, branched_table):

        actual_commands = []

        # noinspection PyUnusedLocal
        def mock_athena_query(mock_self, query):
            actual_commands.append(query)
            if 'ADD PARTITION' in query:
                return {'state': 'FAILED', 'state_change_reason': 'Partition already exists'}
            else:
                return {'state': 'SUCCEEDED', 'state_change_reason': None}

        monkeypatch.setattr(GlueBranch, '_GlueBranch__athena_query', mock_athena_query)
        ans = glue_branch.put_partition(branched_table, 'year=18/month=10/day=25')

        assert "ALTER TABLE {db}.{table}__{branch} ADD PARTITION (year='18', month='10', day='25') location"\
            .format(db=TestTableUtils.db, table=branched_table.name, branch=glue_branch.name) in actual_commands[0]
        assert "ALTER TABLE {db}.{table}__{branch} PARTITION (year='18', month='10', day='25') set location"\
            .format(db=TestTableUtils.db, table=branched_table.name, branch=glue_branch.name) in actual_commands[1]
        assert ans

    def test_fork_branch(self, monkeypatch, glue_branch, branched_table):

        forked_branch_name = '123forked'
        calls = []

        def mock_get_client():
            class MockClient:

                def __init__(self):
                    pass

                def get_table(*args, **kwargs):
                    calls.append({'foo': 'get_table', 'args': args, 'kwargs': kwargs})
                    return {u'Table': {u'StorageDescriptor': {
                        u'Location': u's3://sw-dag-published-v2/{table}/{branch}'
                        .format(table=branched_table, branch=glue_branch.name),
                        u'PartitionKeys': [{u'Type': u'string', u'Name': u'year'},
                                           {u'Type': u'string', u'Name': u'month'},
                                           {u'Type': u'string', u'Name': u'day'}]},
                        u'Name': u'{table}_{branch}'
                            .format(table=branched_table, branch=glue_branch.name)}}

                # noinspection PyMethodMayBeStatic,PyPep8Naming
                def create_table(self, TableInput=None, *args, **kwargs):
                    assert TableInput['Name'] == '{table}__{branch}'\
                        .format(table=branched_table.name, branch=forked_branch_name)
                    calls.append({'foo': 'create_table', 'args': args, 'kwargs': kwargs})

                def get_partitions(*args, **kwargs):
                    calls.append({'foo': 'get_partitions', 'args': args, 'kwargs': kwargs})
                    return {
                        u'Partitions': [{
                            u'StorageDescriptor': {
                                u'Location': u's3://sw-dag-published-v2/{db}/{table}/{branch}'
                                .format(db=TestTableUtils.db, table=branched_table.name, branch=glue_branch.name),
                            },
                            u'TableName': u'{table}__{branch}'.format(table=branched_table.name, branch=glue_branch.name),
                            u'Values': [
                                u'18',
                                u'10',
                                u'25'],
                            u'DatabaseName': u'{db}'.format(db=TestTableUtils.db)}
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

        monkeypatch.setattr(GlueBranch, 'list_dbs',
                            lambda _: [TestTableUtils.db])
        monkeypatch.setattr(GlueBranch, 'list_branchable_tables',
                            lambda _, dbs: {dbs[0]: [branched_table]})

        monkeypatch.setattr(pylib.published_data_lake.branch, 'get_glue_client', mock_get_client)

        ans = glue_branch.fork_branch(forked_branch_name)
        assert ans
        assert map(lambda x: x['foo'], calls) == ['get_table', 'create_table', 'get_partitions',
                                                  'batch_create_partition']
