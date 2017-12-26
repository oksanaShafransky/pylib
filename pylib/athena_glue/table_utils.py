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

