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


def repair_checkpointed_table(db, table_name):
    athena_client = get_athena_client()
    response = athena_client.start_query_execution(
        QueryString='msck repair table {}'.format(table_name),
        QueryExecutionContext={
            'Database': '{}'.format(db)
        },
        ResultConfiguration={
            'OutputLocation': 's3://sw-dag-published-v2/tmp/',
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )
    query_execution_id = response['QueryExecutionId']

    def query_finished(response):
        return response['QueryExecution']['Status']['State'] in ['SUCCEEDED','FAILED','CANCELLED']

    terminal_response = polling.poll(
        lambda: athena_client.get_query_execution(QueryExecutionId=query_execution_id),
        check_success=query_finished, step=0.5, timeout=7)
    assert terminal_response['QueryExecution']['Status']['State'] == 'SUCCEEDED'
    return True


def create_new_checkpoint(db, table_name, new_checkpoint_id):
    client = get_glue_client()
    existing_table_def_response = client.get_table(
        DatabaseName='{}'.format(db),
        #Name='{}_latest'.format(checkpointed_table_name)
        Name=table_name
    )
    existing_table_def = existing_table_def_response['Table']
    new_table_def = __filter_out_dict(existing_table_def, ['UpdateTime', 'CreatedBy', 'CreateTime'])
    new_table_name = existing_table_def['Name'] + '_' + new_checkpoint_id
    new_table_def['Name'] = new_table_name
    client.create_table(DatabaseName='{}'.format(db),
                        TableInput=new_table_def)

    prev_partitions_response = client.get_partitions(
        DatabaseName='{}'.format(db),
        #TableName='{}_latest'.format(checkpointed_table_name)
        TableName=table_name
    )

    new_partitions_response=client.batch_create_partition(
        DatabaseName='{}'.format(db),
        TableName='{}'.format(new_table_name),
        PartitionInputList=map(lambda d:__filter_out_dict(d,['CreationTime','TableName','DatabaseName']),
                               prev_partitions_response['Partitions'])
    )

    assert not new_partitions_response['Errors']

    new_table_def['Name'] = existing_table_def['Name']
    if 'from_' in existing_table_def['StorageDescriptor']['Location']:
        new_table_location = '{}from_{}/' \
            .format(existing_table_def['StorageDescriptor']['Location'].rsplit('from_',1)[0], new_checkpoint_id)
    else:
        new_table_location = '{}from_{}/' \
            .format(existing_table_def['StorageDescriptor']['Location'], new_checkpoint_id)
    new_table_def['StorageDescriptor']['Location'] = new_table_location
    response = client.update_table(
        DatabaseName='{}'.format(db),
        TableInput=new_table_def
    )

    return response
