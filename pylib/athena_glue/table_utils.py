import boto3
import polling


def get_athena_client():
    client = boto3.client('athena')
    return client


def get_glue_client():
    client = boto3.client('glue')
    return client


def __filter_out_dict(dictionary, keys):
    return {k: v for k, v in dictionary.items() if k not in keys}


def repair_table(db, table_name):
    athena_client = get_athena_client()
    repair_query_response = athena_client.start_query_execution(
        QueryString='msck repair table {}'.format(table_name),
        QueryExecutionContext={
            'Database': db
        },
        ResultConfiguration={
            'OutputLocation': 's3://sw-dag-published-v2/tmp/',
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )
    repair_query_execution_id = repair_query_response['QueryExecutionId']

    def is_query_finished(query_response):
        return query_response['QueryExecution']['Status']['State'] in ['SUCCEEDED', 'FAILED', 'CANCELLED']

    terminal_response = polling.poll(
        lambda: athena_client.get_query_execution(
            QueryExecutionId=repair_query_execution_id),
        check_success=is_query_finished, step=0.5, timeout=7)
    assert terminal_response['QueryExecution']['Status']['State'] == 'SUCCEEDED'
    return True


def create_checkpoint(db, table_name, new_checkpoint_id):
    client = get_glue_client()
    table_def_response = client.get_table(
        DatabaseName=db,
        Name=table_name
    )
    table_def = table_def_response['Table']
    checkpoint_table_def = __filter_out_dict(table_def, ['UpdateTime', 'CreatedBy', 'CreateTime'])
    checkpoint_table_name = '{}_{}'.format(table_def['Name'], new_checkpoint_id)
    checkpoint_table_def['Name'] = checkpoint_table_name
    client.create_table(DatabaseName='{}'.format(db),
                        TableInput=checkpoint_table_def)

    table_partitions_response = client.get_partitions(
        DatabaseName=db,
        TableName=table_name
    )

    checkpoint_table_partitions_response = client.batch_create_partition(
        DatabaseName=db,
        TableName=checkpoint_table_name,
        PartitionInputList=map(lambda d: __filter_out_dict(d, ['CreationTime', 'TableName', 'DatabaseName']),
                               table_partitions_response['Partitions'])
    )
    assert not checkpoint_table_partitions_response['Errors']

    table_new_def = checkpoint_table_def
    table_new_def['Name'] = table_def['Name']
    if 'from_' in table_def['StorageDescriptor']['Location']:
        new_table_location = '{}from_{}/' \
            .format(table_def['StorageDescriptor']['Location'].rsplit('from_', 1)[0], new_checkpoint_id)
    else:
        new_table_location = '{}from_{}/' \
            .format(table_def['StorageDescriptor']['Location'], new_checkpoint_id)
    table_new_def['StorageDescriptor']['Location'] = new_table_location
    response = client.update_table(
        DatabaseName='{}'.format(db),
        TableInput=table_new_def
    )
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    return True
