import boto
import platform

HOSTS_PATH = r"C:\Windows\System32\drivers\etc\hosts" if any(platform.win32_ver()) else 'etc/hosts'

emr_conn = boto.connect_emr()


###################################################
# Adds all nodes of a cluster to the hosts file   #
# The section will be marked so as to identify it #
# easily and later remove
###################################################
def list_cluster_nodes(cluster_id, master_only=False):
    # could use the instance type filter, but it's not reliable enough
    extra_args = {}

    if master_only:
        ig_get = emr_conn.list_instance_groups(cluster_id)
        for ig in ig_get.instancegroups:
            if ig.instancegrouptype.lower() == 'master':
                extra_args = {'instance_group_id': ig.id}
                break

    resp = emr_conn.list_instances(cluster_id, **extra_args)
    while True:
        for instance in resp.instances:
            yield instance

        if hasattr(resp, 'marker'):
            resp = emr_conn.list_instances(cluster_id, marker=resp.marker, **extra_args)
        else:
            break


def cluster_header(cluster_id):
    return '''\
#####################################
####### AWS Cluster %s ##############
########### Start ###################
''' % cluster_id


def cluster_footer(cluster_id):
    return '''\
#####################################
####### AWS Cluster %s ##############
########### End #####################
''' % cluster_id


def node_entry(aws_instance):
    return '%s\t%s' % (aws_instance.privateipaddress, aws_instance.privatednsname)


def register_cluster_nodes(cluster_id, writer, master_only=False):
    writer.write('\n')
    writer.write(cluster_header(cluster_id))
    writer.write('\n')

    for node in list_cluster_nodes(cluster_id, master_only=master_only):
        writer.write(node_entry(node))
        writer.write('\n')

    writer.write('\n')
    writer.write(cluster_footer(cluster_id))
    writer.write('\n')


def filter_cluster_nodes(cluster_id, content):
    header, footer = cluster_header(cluster_id), cluster_footer(cluster_id)
    started, ended = False, False
    curr, prev, prev2 = '', '', ''
    output = ''

    for line in content:
        curr, prev, prev2 = line, curr, prev
        if not started:
            output += line
            if prev2 + prev + curr == header:
                started, ended = True, False
                output = output[:-len(header)]  # clear footer
                continue
        elif not ended:
            if prev2 + prev + curr == footer:
                started, ended = False, True
                continue
        else:
            output += line

    return output


if __name__ == '__main__':
    from argparse import ArgumentParser

    option_parser = ArgumentParser(add_help=False)
    option_parser.add_argument('cluster', help='Cluster ID')
    option_parser.add_argument('op', choices=('add', 'remove'), help='Option (Add, Remove)')
    option_parser.add_argument('-m', '--master-only', dest='master_only', required=False, action='store_true')

    args = option_parser.parse_args()

    if args.op == 'add':
        with open(HOSTS_PATH, 'a') as writer:
            register_cluster_nodes(args.cluster, writer, master_only=args.master_only)
    else:
        with open(HOSTS_PATH, 'r') as reader:
            remaining = filter_cluster_nodes(args.cluster, reader)
        with open(HOSTS_PATH, 'w') as writer:
            writer.write(remaining)
