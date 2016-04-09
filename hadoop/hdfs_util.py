__author__ = 'Felix'

import snakebite.client
from snakebite.errors import FileNotFoundException
import subprocess
import logging

MRP_HDFS_NAMENODE_PORT = 8020
MRP_HDFS_NAMENODE_SERVER = 'active.hdfs-namenode-mrp.service.production'

logger = logging.getLogger('__main__')


# read config to rely on run environment
def create_client():
    return snakebite.client.Client(MRP_HDFS_NAMENODE_SERVER, MRP_HDFS_NAMENODE_PORT, use_trash=False)


# needed because the regular client throws an exception when a parent directory doesnt exist either
def directory_exists(dir_name):
        hdfs_client = create_client()

        try:
            return hdfs_client.test(dir_name, directory=True)
        except FileNotFoundException:
            return False


def upload_file_to_hdfs(file_path, target_path):

    if not directory_exists(target_path):
        mkdir_cmd = 'hadoop fs -mkdir -p %s' % target_path
        subprocess.call(mkdir_cmd.split(' '))

    put_cmd = 'hadoop fs -put %s %s' % (file_path, target_path)
    subprocess.call(put_cmd.split(' '))


def delete_file(path):
    hdfs_client = create_client()
    try:
        res = list(hdfs_client.delete(path, False))
        return len(res) > 0
    except FileNotFoundException:
        logger.warn('asked to delete a non existing directory %s' % path)
        return False


def delete_files(*args):
    for path in args:
        delete_file(path)


def delete_dir(path):
    hdfs_client = create_client()
    try:
        res = list(hdfs_client.delete([path], True))
        return len(res) > 0
    except FileNotFoundException:
        logger.warn('asked to delete a non existing file %s' % path)
        return False


def delete_dirs(*args):
    for path in args:
        delete_dir(path)


def get_size(path):
    hdfs_client = create_client()
    return hdfs_client.count([path]).next()['spaceConsumed']


def test_size(path, min_size_required=None):
    hdfs_client = create_client()
    if min_size_required is not None:
        logger.info('Checking if dir %s exists and is larger than %d...' % (path, min_size_required))
    else:
        logger.info('Checking if dir %s exists...' % path)

    try:
        space_consumed = hdfs_client.count([path]).next()['spaceConsumed']
        if min_size_required is None or space_consumed >= min_size_required:
            logger.info('it does')
            return True
        else:
            logger.info('it does not')
            return False
    except FileNotFoundException:
            logger.info('it does not')
            return False
