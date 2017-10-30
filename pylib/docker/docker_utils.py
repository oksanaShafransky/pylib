import docker
import os

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

