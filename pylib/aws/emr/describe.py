import boto
import json

emr_conn = boto.connect_emr()


"""
def cluster_list_instances(cluster_id, group_id=None):
    emr_conn.
    command = ['aws', 'emr', 'list-instances', '--cluster-id', cluster_id]
    if group_id is not None and group_id != 'all':
        command.append('--instance-group-id')
        command.append(group_id)
    ret = run(command)
    return json.loads(ret)
"""

if __name__ == '__main__':
    for instance in emr_conn.list_instances('j-2DE304ACC301V').instances:
        print instance

    for ig in emr_conn.list_instance_groups('j-2DE304ACC301V').instancegroups:
        print ig.instancegrouptype
