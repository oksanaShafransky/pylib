import subprocess
from subprocess import Popen, PIPE
import json

from pprint import pprint


HOSTS_PATH = r"C:\Windows\System32\drivers\etc\hosts"

def run(param_list):
    process = Popen(param_list, stdout=PIPE, stderr=PIPE)
    out, err = process.communicate()
    print err #TODO print to stderr
    ret = process.returncode
    assert ret == 0, 'Failed to run command: %s\n(exit value %d)' % (' '.join(param_list), ret)
    return out


def cluster_create(cluster_name, start_script):
    ret = run(['bash', start_script])
    cluster_dict= json.loads(ret)
    cluster_id = cluster_dict['ClusterId']
    print 'cluster was created successfuly. id=%s' % cluster_id

    return cluster_dict


def cluster_wait(cluster_id):
    out = run(['aws', 'emr', 'wait', 'cluster-running', '--cluster-id', cluster_id])
    print out


def cluster_desc(cluster_id):
    ret = run(['aws', 'emr', 'describe-cluster', '--cluster-id', cluster_id])
    return json.loads(ret)


def cluster_get_details(cluster_id):
    details = {}

    full_desc = cluster_desc(cluster_id)['Cluster']

    details['id'] = full_desc['Id']
    details['name'] = full_desc['Name']
    details['master_name'] = full_desc['MasterPublicDnsName']
    details['full'] = full_desc

    instances = cluster_list_instances(cluster_id)['Instances']
    details['address_map'] = [
        {'private_name' : inst['PrivateDnsName'],
         'public_ip' : inst['PublicIpAddress']}
        for inst in instances
        if inst['Status']['State'] == u'RUNNING']
    details['full_instances'] = instances

    return details


def cluster_list_instances(cluster_id, group_id = None):
    command = ['aws','emr', 'list-instances', '--cluster-id', cluster_id]
    if group_id is not None and group_id != 'all':
        command.append('--instance-group-id')
        command.append(group_id)
    ret = run(command)
    return json.loads(ret)



def emr_list(state='active'):
    command = ['aws','emr', 'list-clusters']
    if state is not None and state != 'all':
        command.append('--%s' % state)
    ret = run(command)
    return json.loads(ret)

HOSTS_EMR_START_MARKER = '###### EMR CONFIGURATION_START ######\n'
HOSTS_EMR_END_MARKER = '###### EMR CONFIGURATION_END ######\n'



def get_hosts_lines(cluster_obj):
    headline = '### cluster %s - %s ###\n' % (cluster_obj['id'], cluster_obj['name'])
    map_lines = ["%s\t%s\n" % (instance['public_ip'], instance['private_name']) for instance in cluster_obj['address_map']]
    return ['\n'] + [headline] + map_lines


def emr_update_hosts():
    with open(HOSTS_PATH, 'r') as file:
        host_lines = file.readlines()

    #remove old conf. currently just replacing
    if HOSTS_EMR_START_MARKER in host_lines:
        conf_start = host_lines.index(HOSTS_EMR_START_MARKER)
        conf_end = host_lines.index(HOSTS_EMR_END_MARKER)
        host_lines = host_lines[:conf_start] + host_lines[conf_end+1:]


    lines_to_write = []
    for cluster in emr_list()['Clusters']:
        print "creating map for cluster %s (%s)" % (cluster['Name'], cluster['Id'])
        if cluster['Status']['State'] != 'STARTING':
            try:
                lines_to_write += get_hosts_lines(cluster_get_details(cluster['Id']))
            except:
                print "could not get details for this cluster"
        else:
            print "this cluster is still starting"

    with open(HOSTS_PATH, 'w') as file:
        file.writelines(host_lines +
                        [HOSTS_EMR_START_MARKER] +
                        lines_to_write +
                        [HOSTS_EMR_END_MARKER])
    return lines_to_write




if __name__ == '__main__':
    pprint(emr_update_hosts())