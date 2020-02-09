import requests
import json
import boto3
from retry import retry

from pylib.config.SnowflakeConfig import SnowflakeConfig
class MetadatumsClient(object):

    #TODO document with examples 'arn:aws:sns:us-east-1:838192392483:production-metadatums'
    def __init__(self, metadatums_host, sns_topic=None):
        self.metadatums_host = metadatums_host
        self.sns_topic = sns_topic

    @staticmethod
    def from_snowflake(env=None):
        sc = SnowflakeConfig(env)
        return MetadatumsClient(
            metadatums_host=sc.get_service_name(service_name='metadatums'),
            sns_topic=sc.get_service_name(service_name='metadatums-sns-topic')
        )

    def post_hbase_partition_rest(self, table_name, branch, partition, table_full_name):
        request_url = '{metadatums_host}/collections/hbase/{table_name}/partitions'.format(
            metadatums_host=self.metadatums_host,
            table_name=table_name
        )

        res = requests.post(
            request_url,
            json={
                'branch': branch,
                'partition': partition,
                'metadatum': {'table': table_full_name}
            }
        )

        print('Metadatums service response: {}'.format(res.text))
        assert res.ok, "metadatums post request failed" #TODO print locals

    def post_hbase_partition_sns(self, table_name, branch, partition, table_full_name):
        client = boto3.client('sns')

        message = {
            "collection_type": "hbase",
            "collection_id": table_name,
            "branch": branch,
            "partition": partition,
            "metadatums": {"table": table_full_name}
        }

        ret = client.publish(TopicArn=self.sns_topic, Message=json.dumps(message))
        print("posted new partition to sns (message id: {message_id})".format(message_id=ret['MessageId']))


    def post_hbase_partition(self, table_name, branch, partition, table_full_name, skip_sns=False):
        """
        adds a metadatums record for hbase table

        Args:
            table_name: collection name in hbase (example: top_lists)
            branch: base branch - inheriting branches will be updated automatically (example: 0c04f38)
            partition: the collection's partition - represents the date (example: top_lists_last-28_19_07_14)
            table_full_name: The ectual table name in hbase (example: "0c04f38_top_lists_last-28_19_07_14)
        """
        if skip_sns:
            self.post_hbase_partition_rest(table_name, branch, partition, table_full_name)
        else:
            assert self.sns_topic is not None, "sns topic not set"
            self.post_hbase_partition_sns(table_name, branch, partition, table_full_name)




