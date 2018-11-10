import boto3


def get_s3_client(access_key=None, secret_key=None):
    return boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )


def put_object_tag(client, bucket, key, tag_key, tag_value):
    client.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={
            'TagSet':
                [{
                    'Key': tag_key,
                    'Value': tag_value
                }]
        }
    )


def delete_object_tagging(client, bucket, key):
    client.delete_object_tagging(
        Bucket=bucket,
        Key=key
    )


def list_objects(client, bucket, prefix):
    paginator = client.get_paginator('list_objects')
    operation_parameters = {
        'Bucket': bucket,
        'Prefix': prefix
    }

    all_keys = []
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        all_keys += map(lambda c: c['Key'], page['Contents'])

    return all_keys
