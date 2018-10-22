import logging
import subprocess

import math
import six
import snakebite.client
from snakebite.errors import FileNotFoundException
from pylib.config.SnowflakeConfig import SnowflakeConfig

__author__ = 'Felix'

MRP_HDFS_NAMENODE_PORT = 8020
env_namenode_hostname = None

logger = logging.getLogger(__name__)

#read from snowflake only once to reduce number of calls to server (assumes sf env does not change during runtime)
def namenode_from_env():
    global env_namenode_hostname
    if env_namenode_hostname is None:
        env_namenode_hostname = SnowflakeConfig().get_service_name(service_name='active.hdfs-namenode')
    return env_namenode_hostname


# read config to rely on run environment
def create_client(name_node=None, port=None):
    name_node = name_node or namenode_from_env()
    port = port or MRP_HDFS_NAMENODE_PORT
    return snakebite.client.Client(name_node, port, use_trash=True)


def server_defaults(hdfs_client=None, force_reload=False):
    if not hdfs_client:
        hdfs_client = create_client()
    return hdfs_client.serverdefaults(force_reload)


def calc_desired_partitions(dir_name):
    dfs_blocksize = int(server_defaults()['blockSize'])
    print("HDFS Blocksize is %d" % dfs_blocksize)
    path_size = get_size(dir_name)
    return int(math.ceil(float(path_size) / dfs_blocksize))


# needed because the regular client throws an exception when a parent directory doesnt exist either
def directory_exists(dir_name, hdfs_client=None):
    if not hdfs_client:
        hdfs_client = create_client()
    try:
        return hdfs_client.test(dir_name, directory=True)
    except FileNotFoundException:
        return False


def file_exists(file_path, hdfs_client=None):
    if not hdfs_client:
        hdfs_client = create_client()
    try:
        return hdfs_client.test(path=file_path, directory=False)
    except FileNotFoundException:
        return False


def upload_file_to_hdfs(file_path, target_path):
    if not directory_exists(target_path):
        mkdir_cmd = 'hadoop fs -mkdir -p %s' % target_path
        subprocess.call(mkdir_cmd.split(' '))

    put_cmd = 'hadoop fs -put %s %s' % (file_path, target_path)
    subprocess.call(put_cmd.split(' '))


def copy_dir_from_path(src_path, target_path):
    delete_dir(target_path)
    subprocess.call(["hadoop", "fs", "-mkdir", "-p", target_path])
    subprocess.call(("hadoop", "fs", "-cp", "-f", src_path + "/*", target_path))


def copy_file(file_path, target_path):
    if not directory_exists(target_path):
        mkdir_cmd = 'hadoop fs -mkdir -p %s' % target_path
        subprocess.call(mkdir_cmd.split(' '))

    cp_cmd = 'hadoop fs -cp %s %s' % (file_path, target_path)
    subprocess.call(cp_cmd.split(' '))


def append_to_file(file_path, string):
    printf_cmd = subprocess.Popen(('printf', string), stdout=subprocess.PIPE)
    append_cmd = 'hadoop fs -appendToFile - %s' % file_path
    subprocess.call(append_cmd.split(' '), stdin=printf_cmd.stdout)


def delete_file(path, hdfs_client=None):
    if hdfs_client is None:
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


def delete_dir(path, hdfs_client=None):
    # saftey valve in case someone tries to delete '/similargroup' by mistake
    assert path.count('/') > 4, "can't delete programmatically folders this close to root. are you sure you intended to delete %s" % path

    if hdfs_client is None:
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


def move_dir(path, target, hdfs_client=None):
    if hdfs_client is None:
        hdfs_client = create_client()

    hdfs_client.rename([path], target).next()


def get_size(path, with_replicas=False):
    """
    :param path: path to size
    :param with_replicas: whether to consider physical space consumed by all replication or just the canonical size
    :return: size in bytes
    """
    hdfs_client = create_client()
    return hdfs_client.count([path]).next()['spaceConsumed' if with_replicas else 'length']


def test_size(path, min_size_required=None, is_strict=False, with_replicas=False):
    if min_size_required is not None:
        logger.info('Checking that dir %s exists and is larger than %d...' % (path, min_size_required))
    else:
        logger.info('Checking that dir %s exists...' % path)

    try:
        space_consumed = get_size(path, with_replicas=with_replicas)
        if min_size_required is None or space_consumed >= min_size_required:
            logger.info('it does')
            if is_strict:
                logger.info('Checking that dir is not too large...')
                if space_consumed > min_size_required * 30:  # Chang to 10 after fixing spaceConsumed
                    logger.info('Dir %s is %d, which is too large for the check vs %d' % (path, space_consumed, min_size_required))
                    return False
            return True
        else:
            logger.info('it does not. It is actually ' + str(space_consumed))
            return False
    except FileNotFoundException:
        logger.info('it does not')
        return False


def extract_hive_partition_values(paths, column_name):
    assert isinstance(paths, list), "paths parameter should be instance of list, got " + paths
    values = []
    for path in paths:
        path_components = path.split('/')
        for path_component in path_components:
            if '=' in path_component:
                path_component_parts = path_component.split('=')
                if len(path_component_parts) == 2 and column_name == path_component_parts[0]:
                    assert '' != path_component_parts[1], 'Empty partition value is not expected here.' + path
                    values.append(path_component_parts[1])
    return sorted(list(set(values)))


def get_hive_partition_values(base_path, column_name):
    hdfs_client = create_client()
    all_paths = [v['path'] for v in hdfs_client.ls([base_path], recurse=True, include_toplevel=True)]
    relevant_paths = filter(lambda p: '/_' not in p and '/.' not in p, all_paths)
    return extract_hive_partition_values(relevant_paths, column_name)


def get_hive_partitions(base_path):

    if base_path[-1] != '/':
        base_path='{}/'.format(base_path)

    def get_hive_partitions_inner(base_path):
        partitions = []
        hdfs_client = create_client()
        all_paths = [v['path'] for v in hdfs_client.ls([base_path], recurse=False, include_toplevel=False)]
        is_empty_path = len(all_paths)==0
        relevant_paths = filter(lambda p: '/_' not in p and '/.' not in p and '.gz' not in p, all_paths)
        nested_paths_are_children = False
        for path in relevant_paths:
            path_component_to_check = path.split('/')[-1]
            if '=' in path_component_to_check:
                path_component_to_check_parts = path_component_to_check.split('=')
                if len(path_component_to_check_parts) == 2:
                    assert '' != path_component_to_check_parts[1], 'Empty partition value is not expected here.' + path
                    if nested_paths_are_children:
                        partitions += [path]
                    else:
                        new_partitions, is_empty_path = get_hive_partitions_inner(path)
                        if len(new_partitions) == 0 and not is_empty_path:
                            nested_paths_are_children = True
                            partitions += [path]
                        else:
                            partitions += new_partitions
        return partitions, is_empty_path

    paths = get_hive_partitions_inner(base_path)[0]
    return [path.split(base_path,1)[-1] for path in paths]


def list_dirs(paths, hdfs_client=None):
    if not hdfs_client:
        hdfs_client = create_client()
    return [child['path'] for child in hdfs_client.ls([paths] if not isinstance(paths, list) else paths) if
            child['file_type'] == 'd']


def list_files(paths, hdfs_client=None):
    if not hdfs_client:
        hdfs_client = create_client()
    return [child['path'] for child in hdfs_client.ls([paths] if not isinstance(paths, list) else paths) if
            child['file_type'] == 'f']


def list_files_with_size(paths, hdfs_client=None):
    if not hdfs_client:
        hdfs_client = create_client()
    return {
        child['path']: child['length']
        for child in hdfs_client.ls([paths] if not isinstance(paths, list) else paths)
        if child['file_type'] == 'f'
    }


def count_files(path, hdfs_client=None):
    if not hdfs_client:
        hdfs_client = create_client()
    return hdfs_client.count([path]).next()['fileCount']


def read_files(paths, hdfs_client=None):
    if not hdfs_client:
        hdfs_client = create_client()
    return ''.join(hdfs_client.text(paths))


def get_file(file_path, local_name, hdfs_client=None):
    if not hdfs_client:
        hdfs_client = create_client()
    return hdfs_client.copyToLocal([file_path], local_name).next()


def check_success(directory):
    hdfs_client = create_client()
    logging.info("Checking that dir '%s' contains _SUCCESS..." % directory)
    try:
        res = hdfs_client.test(path=(directory + "/_SUCCESS"))
        logging.info('it does' if res else "it doesn't")
        return res
    except FileNotFoundException:
        logger.info('directory does not exist')
        return False


def mark_success(dir_path, create_missing_path=False):
    hdfs_client = create_client()
    if create_missing_path:
        mkdir(dir_path, hdfs_client)

    if not isinstance(dir_path, six.string_types):
        raise Exception('if you want different type to be supported, implement it yourself')

    hdfs_client.touchz(paths=[dir_path + '/_SUCCESS']).next()


def mkdir(dir_path, hdfs_client=None):
    if not hdfs_client:
        hdfs_client = create_client()
    hdfs_client.mkdir(paths=[dir_path], create_parent=True).next()


def change_file_extension(path, new_ext, hdfs_client=None):
    hdfs_client = hdfs_client or create_client()
    if directory_exists(path, hdfs_client):
        for dir_file in list_files(path):
            change_file_extension(dir_file, new_ext, hdfs_client)
    elif file_exists(path, hdfs_client):
        last_dot = path.rfind('.')
        new_name = (path[:last_dot] if last_dot > 0 else path) + '.' + new_ext
        hdfs_client.rename([path], new_name).next()


class HdfsApi(object):
    """
    An OO wrapper for utility functions from pylib.hadoop.hdfs_util.
    The idea is to keep using the same hdfs client instead of recreating it every call.
    """

    def __init__(self):
        self.client = create_client()

    def list_files(self, dir):
        return list_files(dir, self.client)

    def get_file(self, path, local_path):
        return get_file(path, local_path, self.client)

    def delete_file(self, path):
        return delete_file(path, self.client)

    def delete_dir(self, path):
        return delete_dir(path, self.client)

    def exists(self, path, is_dir=False):
        try:
            return self.client.test(path=path, directory=is_dir)
        except FileNotFoundException:
            return False

    def exists_dir(self, path):
        return self.exists(path, is_dir=True)

    def exists_file(self, path):
        return self.exists(path, is_dir=False)

    @staticmethod
    def copy_from_local(local_path, hdfs_path):
        HdfsApi._cmd_exec_helper(['hdfs', 'dfs', '-copyFromLocal', local_path, hdfs_path])

    @staticmethod
    def put(local_path, hdfs_path):
        HdfsApi._cmd_exec_helper(['hdfs', 'dfs', '-put', local_path, hdfs_path])

    @staticmethod
    def put_force(local_path, hdfs_path):
        HdfsApi._cmd_exec_helper(['hdfs', 'dfs', '-put', '-f', local_path, hdfs_path])

    @staticmethod
    def _cmd_exec_helper(cmd_args):
        cmd_res = subprocess.call(cmd_args)
        if cmd_res != 0:
            raise Exception('Command returned non zero value ({}): {}'.format(cmd_res, cmd_args))

    @staticmethod
    def upload_file_to_hdfs(file_path, target_path):
        upload_file_to_hdfs(file_path, target_path)
