import boto3
import polling


def get_athena_client():
    client = boto3.client('athena')
    return client


def get_glue_client():
    client = boto3.client('glue')
    return client


def repair_table(db, table_name):
    client = get_athena_client()
    response = client.start_query_execution(
        QueryString='msck repair table {table}'.format(table=table_name),
        QueryExecutionContext={
            'Database': '{database}'.format(database=db)
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
        lambda: client.get_query_execution(
            QueryExecutionId=query_execution_id
        ),
        check_success=query_finished,
        step=0.5,
        timeout=7)
    return terminal_response['QueryExecution']['Status']['State'] == 'SUCCEEDED'


def create_new_version(db, table_name, prev_version, new_version):
    client = get_glue_client()
    response = client.get_table(
        DatabaseName='{db}'.format(db=db),
        Name='{table_name}'.format(table_name=table_name)
    )
    existing_table_def = response['Table']
    new_table_def = existing_table_def
    del new_table_def['UpdateTime']
    del new_table_def['CreatedBy']
    del new_table_def['CreateTime']
    new_table_name = existing_table_def['Name'] + '_' + new_version
    new_table_def['Name'] = new_table_name
    new_table_def['StorageDescriptor']['Location'] =\
        existing_table_def['StorageDescriptor']['Location'] + new_version + '/'
    client.create_table(DatabaseName='{db}'.format(db=db),
                        TableInput=new_table_def)

    assert repair_table(db, table_name)

    prev_partitions_response = client.get_partitions(
        DatabaseName='{db}'.format(db=db),
        TableName='{table_name}'.format(table_name=table_name)
    )

    new_partitions_response=client.batch_create_partition(
        DatabaseName='{db}'.format(db=db),
        TableName='{table_name}'.format(table_name=new_table_name),
        PartitionInputList=
        map(lambda d: {k:v for k, v in d.items()
                       if k not in ['CreationTime','TableName','DatabaseName']},
            prev_partitions_response['Partitions'])
    )

    return not new_partitions_response['Errors']
