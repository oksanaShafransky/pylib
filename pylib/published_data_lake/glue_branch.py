import boto3
from pylib.published_data_lake.branch import *
from retry import retry
from botocore import exceptions

def get_glue_client():
    client = boto3.client('glue', region_name='us-east-1')
    return client


def get_athena_client():
    client = boto3.client('athena', region_name='us-east-1')
    return client


def _athena_query(query):
    athena_client = get_athena_client()
    query_response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': 's3://sw-dag-published-v2/tmp/',
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )
    query_id = query_response['QueryExecutionId']

    @retry(tries=10, delay=0.5, logger=None)
    def query_response_state():
        execution_status_query_response = athena_client.get_query_execution(
            QueryExecutionId=query_id)
        status = execution_status_query_response['QueryExecution']['Status']
        state = status['State']
        state_change_reason = status['StateChangeReason'] if 'StateChangeReason' in status else None

        assert state in ['SUCCEEDED', 'FAILED', 'CANCELLED']
        return {'state': state,
                'state_change_reason': state_change_reason,
                'output_location': execution_status_query_response['QueryExecution']['ResultConfiguration']['OutputLocation']}

    query_response_state = query_response_state()
    return query_response_state


class GlueBranch(Branch):

    def __init__(self, *args, **kwargs):
        super(GlueBranch, self).__init__(*args, **kwargs)

    def list_branchable_tables(self, dbs):
        ans = {}
        for db in dbs:
            ans_db = []
            client = get_glue_client()
            response = client.get_tables(
                DatabaseName=db,
                Expression='*__{}'.format(self.name)
            )
            assert response
            for table_def in response['TableList']:
                ans_db += [BranchableTable(db=db,
                                           name=table_def['Name'].split('__{}'.format(self.name))[0])]
            ans[db] = ans_db
        return ans

    def put_partition(self, branchable_table, partition, create_new_table_if_missing=True):
        table = self._fully_qualified_table_name(branchable_table)
        partition_sql = ', '.join(["{}='{}'".format(kv.split('=')[0], kv.split('=')[1])
                                   for kv in partition.split('/')])
        query_response = _athena_query("ALTER TABLE {table} ADD PARTITION ({partition_sql}) "
                                             "location '{location}'"
                                             .format(table=table,
                                                     location=self._table_location(branchable_table),
                                                     partition_sql=partition_sql))
        logger.debug('Put partition query response is {}'.format(query_response))
        if query_response['state'] == 'SUCCEEDED':
            return True

        elif query_response['state'] == 'FAILED' and \
                'Partition already exists' in query_response['state_change_reason']:
            # query_response = \
            #     athena_query("ALTER TABLE {table} PARTITION ({partition_sql}) "
            #                         "set location '{location}';"
            #                         .format(table=table,
            #                                 location=self._table_location(branchable_table),
            #                                 partition_sql=partition_sql))
            # if query_response['state'] == 'SUCCEEDED':
            #     return True
            client = get_glue_client()
            response = client.get_partition(
                DatabaseName=branchable_table.db,
                TableName=self._table_name(branchable_table),
                PartitionValues=[kv.split('=')[1] for kv in partition.split('/')]
            )
            assert response['ResponseMetadata']['HTTPStatusCode'] == 200
            partition_input = response['Partition']
            partition_input = filter_out_dict(partition_input, ['CreationTime', 'TableName', 'DatabaseName'])
            partition_input['StorageDescriptor']['Location'] = self._table_location(branchable_table)
            response = client.update_partition(DatabaseName=branchable_table.db,
                                               TableName=self._table_name(branchable_table),
                                               PartitionValueList=[kv.split('=')[1] for kv in partition.split('/')],
                                               PartitionInput=partition_input)
            logger.debug('Update partition query response is {}'.format(response))
            assert response['ResponseMetadata']['HTTPStatusCode'] == 200
            return True

        elif query_response['state'] == 'FAILED' and \
                'Table not found' in query_response['state_change_reason'] \
                and create_new_table_if_missing:
            self._create_new_table(branchable_table)
            return self.put_partition(branchable_table, partition, create_new_table_if_missing=False)
        return False

    def _create_new_table(self, branchable_table):
        client = get_glue_client()
        crawler_name = 'PublicDataLake_{}.{}'.format(branchable_table.db,
                                                     branchable_table.name)
        response = client.create_crawler(
            Name=crawler_name,
            Role='arn:aws:iam::838192392483:role/AWSGlueServiceRole-Dag-Published-Data-Lake-Read',
            DatabaseName=branchable_table.db,
            Targets={'S3Targets':
                [{
                    'Path': self._table_location(branchable_table)
                }]
            },
            TablePrefix=crawler_name
        )
        logger.debug('Create crawler response is {}'.format(response))
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        response = client.start_crawler(
            Name=crawler_name
        )

        @retry(tries=60, delay=10, logger=None)
        def crawl_status():
            execution_status_query_response = \
                client.get_crawler(Name=crawler_name)
            status = execution_status_query_response['Crawler']
            state = status['State']
            last_crawl_status = status['LastCrawl']['Status'] if 'LastCrawl' in status else None
            assert state in ['READY']
            return last_crawl_status

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        assert crawl_status() == 'SUCCEEDED'
        logger.debug('Crawl status is {}'.format(response))
        response = client.delete_crawler(Name=crawler_name)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        temp_table_name = crawler_name + self.name
        table_def_response = client.get_table(
            DatabaseName=branchable_table.db,
            Name=temp_table_name
        )
        table_def = table_def_response['Table']
        self_table_def = filter_out_dict(table_def, ['UpdateTime', 'CreatedBy', 'CreateTime'])
        self_table_def['Name'] = self._table_name(branchable_table)
        response = client.create_table(DatabaseName=branchable_table.db,
                                       TableInput=self_table_def)
        logger.debug('Create table response is {}'.format(response))
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        response = client.delete_table(
            DatabaseName=branchable_table.db,
            Name=temp_table_name
        )
        logger.debug('Delete table response is {}'.format(response))
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    def fork_branch(self, new_branch_name, dbs=None):
        if dbs is None:
            dbs = self.list_dbs()
        new_branch = GlueBranch(new_branch_name)
        return new_branch.pull_from_branch(self, dbs)

    def pull_from_branch(self, reference_branch, dbs):
        branchable_tables = reference_branch.list_branchable_tables(dbs)
        for db in branchable_tables:
            db_branchable_tables = branchable_tables[db]
            for branchable_table in db_branchable_tables:
                table_pull_succeeded = self._pull_table_from_branch(branchable_table, reference_branch)
                assert table_pull_succeeded
        return True

    def _pull_table_from_branch(self, branchable_table, reference_branch):
        client = get_glue_client()
        reference_table_name = reference_branch._table_name(branchable_table)
        table_def_response = client.get_table(
            DatabaseName=branchable_table.db,
            Name=reference_table_name
        )
        table_def = table_def_response['Table']
        self_table_def = filter_out_dict(table_def, ['UpdateTime', 'CreatedBy', 'CreateTime'])
        self_table_name = self._table_name(branchable_table)
        self_table_def['Name'] = self_table_name
        self_table_def['StorageDescriptor']['Location'] = self._table_location(branchable_table)
        try:
            client.create_table(DatabaseName='{}'.format(branchable_table.db),
                                TableInput=self_table_def)
        except exceptions.ClientError as e:
            if not e.response['Error']['Code'] == 'AlreadyExistsException':
                raise e

        table_partitions_response = client.get_partitions(
            DatabaseName=branchable_table.db,
            TableName=reference_table_name
        )

        checkpoint_table_partitions_response = client.batch_create_partition(
            DatabaseName=branchable_table.db,
            TableName=self_table_name,
            PartitionInputList=map(lambda d: filter_out_dict(d, ['CreationTime', 'TableName', 'DatabaseName']),
                                   table_partitions_response['Partitions'])
        )

        assert checkpoint_table_partitions_response['ResponseMetadata']['HTTPStatusCode'] == 200
        if 'Errors' in checkpoint_table_partitions_response:
            errors = checkpoint_table_partitions_response['Errors']
            for error in errors:
                assert error['ErrorDetail']['ErrorCode'] == 'AlreadyExistsException'
        return True

    @staticmethod
    def list_dbs():
        client = get_glue_client()
        databases_response = client.get_databases()
        assert databases_response['ResponseMetadata']['HTTPStatusCode'] == 200
        all_dbs = databases_response['DatabaseList']
        return [db['Name'] for db in all_dbs if PUBLISHED_DATA_LAKE_DB_PREFIX in db['Name']]
