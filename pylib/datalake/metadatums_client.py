import requests
import json
import boto3
import time

from pylib.config.SnowflakeConfig import SnowflakeConfig


METADATUMS_UPDATE_WAIT_TIME = 120 #time in seconds to wait for metadatums update to tke place

class MetadatumsClient(object):
    def __init__(self, metadatums_host, sns_topic=None, sns_region=None):
        """
        create a metadatums client

        Args:
            metadatums_host: rest host for metadatums (example: metadatums-production.op-us-east-1.web-grid.int.similarweb.io)
            sns_topic: topic for posting new partitions. - required only when performing "write" operations. (example: arn:aws:sns:us-east-1:838192392483:production-metadatums)
        """
        self.metadatums_host = metadatums_host
        self.sns_topic = sns_topic
        self.sns_client = boto3.client('sns', region_name=sns_region)

    @staticmethod
    def from_snowflake(env=None):
        sc = SnowflakeConfig(env)
        config = json.loads(sc.get_service_name(service_name='metadatums_new'))
        return MetadatumsClient(
            metadatums_host=config.get('host'),
            sns_topic=config.get('sns_topic'),
            sns_region=config.get('sns_region')
        )

    def get_hbase_table_name(self, table_name, branch, partition):
        """
        get the physical hbase tbale name for a given entry in metadatums

        Args:
            table_name: collection name in hbase (example: top_lists)
            branch: branchstack branch (example: 0c04f38)
            partition: the collection's partition - represents the date (example: top_lists_last-28_19_07_14)

            :returns hbase table name. None if the requested metadateum does not exist
        """
        request_url = 'http://{metadatums_host}/query'.format(
            metadatums_host=self.metadatums_host
        )

        request_data = {
            'query': {
                'collection_type': 'hbase',
                'branch': branch,
                'collection_id': table_name,
                'partition': partition
            }
        }
        res = requests.get(request_url, json=request_data)
        res.raise_for_status()

        # filter out collection_id and branch because the backend does not do it yet
        partitions = res.json()['partitions']['hbase']
        selected_partitions = filter(
            lambda p: p['branch'] == branch and
                      p['collection_id'] == table_name and
                      p['partition'] == partition,
            partitions
        )

        if len(selected_partitions) == 0:
            return None
        else:
            # should be only one
            assert len(selected_partitions) == 1, "duplicate metadatums found for query: {query}. results: {partitions}".format(
                query=request_data, partitions=selected_partitions)
            return selected_partitions[0]['metadatum']['table']

    def _post_hbase_partition_rest(self, table_name, branch, partition, table_full_name):
        request_url = 'http://{metadatums_host}/collections/hbase/{table_name}/partitions'.format(
            metadatums_host=self.metadatums_host,
            table_name=table_name
        )

        request_data = {
            'branch': branch,
            'partition': partition,
            'metadatum': {'table': table_full_name}
        }
        print('posting to: {request_url}. payload: {request_data}'.format(request_url=request_url, request_data=request_data))
        res = requests.post(request_url, json=request_data)

        print('Metadatums service response:\n{}'.format(res.text))
        res.raise_for_status()

    def _publish_hbase_partition_sns(self, table_name, branch, partition, table_full_name):
        message = {
            "message_type": "add_partition",
            "collection_type": "hbase",
            "collection_id": table_name,
            "branch": branch,
            "partition": partition,
            "metadatums": {"table": table_full_name}
        }
        ret = self.sns_client.publish(TopicArn=self.sns_topic, Message=json.dumps(message))
        print("posted new partition to sns (message id: {message_id})".format(message_id=ret['MessageId']))

    def publish_hbase_partition(self, table_name, branch, partition, table_full_name, skip_sns=False):
        """
        adds a metadatums record for hbase table

        Args:
            table_name: collection name in hbase (example: top_lists)
            branch: base branch - inheriting branches will be updated automatically (example: 0c04f38)
            partition: the collection's partition - represents the date (example: top_lists_last-28_19_07_14)
            table_full_name: The ectual table name in hbase (example: "0c04f38_top_lists_last-28_19_07_14)
            skip_sns: default is False. if set to true, will post directly to metadatums service, bypassing the sns topic.
            posting through sns is important when deploying production partitions. skip this step only if you know what you are doing
        """
        if skip_sns:
            self._post_hbase_partition_rest(table_name, branch, partition, table_full_name)
        else:
            assert self.sns_topic is not None, "sns topic not set"
            self._publish_hbase_partition_sns(table_name, branch, partition, table_full_name)

        # wait for the partition to appear in metadatums
        current_res = None
        wait_start_time = time.time()
        while current_res != table_full_name:
            assert time.time() - wait_start_time < METADATUMS_UPDATE_WAIT_TIME, "timeout while waiting for metadatums to update"
            time.sleep(10)
            current_res = self.get_hbase_table_name(table_name, branch, partition)
            print("waiting for the partition to appear in metadatums. (current respons: {})".format(current_res))

        print("update complete")

    def _delete_hbase_partition_rest(self, table_name, branch, partition):
        request_url = 'http://{metadatums_host}/collections/hbase/{table_name}/partitions'.format(
            metadatums_host=self.metadatums_host,
            table_name=table_name
        )

        request_data = {
            'branch': branch,
            'partition': partition
        }
        print('deleting: {request_url}. payload: {request_data}'.format(request_url=request_url, request_data=request_data))
        res = requests.delete(request_url, json=request_data)

        print('Metadatums service response:\n{}'.format(res.text))
        res.raise_for_status()

    def _delete_hbase_partition_sns(self, table_name, branch, partition):
        message = {
            "message_type": "drop_partition",
            "collection_type": "hbase",
            "collection_id": table_name,
            "branch": branch,
            "partition": partition
        }
        ret = self.sns_client.publish(TopicArn=self.sns_topic, Message=json.dumps(message))
        print("deleted partition via sns (message id: {message_id})".format(message_id=ret['MessageId']))

    def delete_hbase_partition(self, table_name, branch, partition, skip_sns=False, check_deletion=True):
        """
        deletes a metadatums record for hbase table

        Args:
            table_name: collection name in hbase (example: top_lists)
            branch: base branch - inheriting branches will be updated automatically (example: 0c04f38)
            partition: the collection's partition - represents the date (example: top_lists_last-28_19_07_14)
            skip_sns: default is False. if set to true, will post directly to metadatums service, bypassing the sns topic.
            posting through sns is important when deploying production partitions. skip this step only if you know what you are doing
            check_deletion: default is true. If set to true, will wait 10 seconds and check that the partition was
        """
        current_res = self.get_hbase_table_name(table_name, branch, partition)

        # assert current_res is not None, "entry not found in metadatums: ({}, {}, {})".format(table_name, branch, partition)
        if current_res is None:
            print('entry not found in metadatums: ({}, {}, {}), skipping delete'.format(table_name, branch, partition))
        else:
            if skip_sns:
                self._delete_hbase_partition_rest(table_name, branch, partition)
            else:
                assert self.sns_topic is not None, "sns topic not set"
                self._delete_hbase_partition_sns(table_name, branch, partition)

            # wait for the deletion
            if check_deletion:
                wait_start_time = time.time()
                while current_res is not None:
                    assert time.time() - wait_start_time < METADATUMS_UPDATE_WAIT_TIME, "timeout while waiting for metadatums to update"
                    time.sleep(10)
                    current_res = self.get_hbase_table_name(table_name, branch, partition)
                    print("waiting for the partition to be deleted from metadatums. (current respons: {})".format(current_res))
                print("update complete")

