__author__ = 'Felix'

import os
import pickle
import tempfile
import gzip
import bz2
from stats import check_output

CACHE_FILES_ENV = 'cached_files'


def find_files(path, prefix=''):
    dir_name = prefix + path.split('/')[-1:][0]

    ls_cmd = ['hadoop', 'fs', '-ls', path]
    ls = check_output(ls_cmd)

    files = []
    for line in ls.split('\n'):
        if path in line:
            file_name = line.split(' ')[-1:][0]
            if not os.path.basename(file_name).startswith('_'):
                files += ['%s/%s' % (dir_name, file_name.split('/')[-1:][0])]

    print 'files to cache for path %s: %s' % (path, files)
    return dir_name, files


def cache_files_cmd(files, key=''):
    return 'export %s_%s=%s' % (CACHE_FILES_ENV, key, ','.join(files))


def get_cached_files(key=''):
    import os
    key_env = '%s_%s' % (CACHE_FILES_ENV, key)

    if key_env not in os.environ:
        return []
    else:
        return os.environ[key_env].split(',')


def cache_obj(key, obj):
    obj_tmp_file = tempfile.NamedTemporaryFile(delete=False)
    pickle.dump(obj, obj_tmp_file)
    return obj_tmp_file.name, 'export %s_%s=%s' % (CACHE_FILES_ENV, key, obj_tmp_file.name.split('/')[-1:][0])


def get_cached_object(key):
    key_env = '%s_%s' % (CACHE_FILES_ENV, key)
    if key_env not in os.environ:
        return None
    else:
        obj_file = open(os.environ[key_env])
        obj = pickle.load(obj_file)
        obj_file.close()
        return obj


def open_file(file_name):
    ext = os.path.splitext(file_name)[1]
    if ext == '.gz':
        return gzip.open(file_name)
    elif ext == '.bz2':
        return bz2.BZ2File(file_name)
    elif ext == '.snappy':
        raise TypeError('snappy files are currently not supported')
    else:
        return open(file_name)