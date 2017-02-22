from subprocess import Popen, PIPE
import json

from pprint import pprint


HOSTS_PATH = r"C:\Windows\System32\drivers\etc\hosts"


def run(param_list):
    process = Popen(param_list, stdout=PIPE, stderr=PIPE)
    out, err = process.communicate()
    print err  # TODO print to stderr
    ret = process.returncode
    assert ret == 0, 'Failed to run command: %s\n(exit value %d)' % (' '.join(param_list), ret)
    return out


def cluster_create(cluster_name, start_script):
    ret = run(['bash', start_script])
    cluster_dict = json.loads(ret)
    cluster_id = cluster_dict['ClusterId']
    print 'cluster was created successfully. id=%s' % cluster_id

    return cluster_dict


def cluster_wait(cluster_id):
    out = run(['aws', 'emr', 'wait', 'cluster-running', '--cluster-id', cluster_id])
    print out

