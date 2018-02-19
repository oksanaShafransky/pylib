import boto3
from retry import retry
from botocore import exceptions


def get_glue_client():
    client = boto3.client('glue')
    return client


def get_athena_client():
    client = boto3.client('athena')
    return client


def filter_out_dict(dictionary, keys):
    return {k: v for k, v in dictionary.items() if k not in keys}


class GlueBranch(object):

    def __init__(self, db, name):
        self.db = db
        self.name = name

    def list_branchable_tables(self):
        ans = []
        client = get_glue_client()
        response = client.get_tables(
            DatabaseName=self.db,
            Expression='*_{}'.format(self.name)
        )
        assert response
        for table_def in response['TableList']:
            ans += [table_def['Name'].split('_{}'.format(self.name))[0]]
        return ans

    def __athena_query(self, query):
        athena_client = get_athena_client()
        query_response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': self.db
            },
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
            return {'state': state, 'state_change_reason': state_change_reason}

        query_response_state = query_response_state()
        return query_response_state

    def put_partition(self, branchable_table, partition):
        table = self.__table_name(branchable_table)
        partition_sql = ', '.join(["{}='{}'".format(kv.split('=')[0], kv.split('=')[1])
                                   for kv in partition.split('/')])
        query_response_state = self.__athena_query("ALTER TABLE {table_name} ADD PARTITION ({partition_sql}) "
                                                   "location '{location}'"
                                                   .format(table_name=table,
                                                           location=self.__table_location(branchable_table),
                                                           partition_sql=partition_sql))
        if query_response_state['state'] == 'SUCCEEDED':
            return True

        elif query_response_state['state'] == 'FAILED' and \
                'Partition already exists' in query_response_state['state_change_reason']:
            query_response_state = \
                self.__athena_query("ALTER TABLE {table_name} PARTITION ({partition_sql}) "
                                    "set location '{location}'"
                                    .format(table_name=table,
                                            location=self.__table_location(branchable_table),
                                            partition_sql=partition_sql))
            if query_response_state['state'] == 'SUCCEEDED':
                return True

        return False

    def fork_branch(self, new_branch_name):
        new_branch = GlueBranch(self.db, new_branch_name)
        return new_branch.pull_from_branch(self)

    def pull_from_branch(self, reference_branch):
        for branchable_table in reference_branch.list_branchable_tables():
            table_pull_succeeded = self.__pull_table_from_branch(branchable_table, reference_branch)
            assert table_pull_succeeded
        return True

    def __pull_table_from_branch(self, branchable_table, reference_branch):
        client = get_glue_client()
        reference_table_name = reference_branch.__table_name(branchable_table)
        table_def_response = client.get_table(
            DatabaseName=self.db,
            Name=reference_table_name
        )
        table_def = table_def_response['Table']
        self_table_def = filter_out_dict(table_def, ['UpdateTime', 'CreatedBy', 'CreateTime'])
        self_table_name = self.__table_name(branchable_table)
        self_table_def['Name'] = self_table_name
        self_table_def['StorageDescriptor']['Location'] = self.__table_location(branchable_table)
        try:
            client.create_table(DatabaseName='{}'.format(self.db),
                                TableInput=self_table_def)
        except exceptions.ClientError as e:
            if not e.response['Error']['Code'] == 'AlreadyExistsException':
                raise e

        table_partitions_response = client.get_partitions(
            DatabaseName=self.db,
            TableName=reference_table_name
        )

        checkpoint_table_partitions_response = client.batch_create_partition(
            DatabaseName=self.db,
            TableName=self_table_name,
            PartitionInputList=map(lambda d: filter_out_dict(d, ['CreationTime', 'TableName', 'DatabaseName']),
                                   table_partitions_response['Partitions'])
        )

        print(checkpoint_table_partitions_response)
        assert checkpoint_table_partitions_response['ResponseMetadata']['HTTPStatusCode'] == 200
        if 'Errors' in checkpoint_table_partitions_response:
            errors = checkpoint_table_partitions_response['Errors']
            for error in errors:
                assert error['ErrorDetail']['ErrorCode'] == 'AlreadyExistsException'
        return True

    def __table_name(self, branchable_table):
        return '{}_{}'.format(branchable_table, self.name)

    def __table_location(self, branchable_table):
        return 's3://sw-dag-published-v2/{}/{}'.format(branchable_table, self.name)
