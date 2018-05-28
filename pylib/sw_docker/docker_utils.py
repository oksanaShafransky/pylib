import docker
import os
import xml.etree.ElementTree as ET

DOCKER_HOST_PORT = '2375'


def get_my_docker_gate():
    return os.environ.get('DOCKER_GATE')


def get_my_container_id():
    return os.environ.get('HOSTNAME')


def docker_image_full_name():
    docker_gate = get_my_docker_gate()
    if docker_gate is None:
        return None

    cli = docker.Client('%s:%s' % (docker_gate, DOCKER_HOST_PORT), version='auto')

    return cli.inspect_container(get_my_container_id())['Config']['Image']


def get_namenode_url_from_hdfs_site(hdfs_site_path='/etc/hadoop/conf/hdfs-site.xml'):
    for prop in ET.parse(hdfs_site_path).findall("property"):
        if 'dfs.namenode.rpc-address' in prop.find('name').text:
            namenode_url = prop.find('value').text
            return namenode_url
