import binascii

__author__ = 'Felix'

import os
import sys
import binascii


def encode_env(s):
    return 'v' + binascii.b2a_hex(s)


def decode_env(s):
    if s[0] == 'v':
        return binascii.a2b_hex(s[1:])


# TODO - find a way to pass logger
def load_class(class_name):
    parts = class_name.split('.')
    module = ".".join(parts[:-1])
    class_name = parts[-1:]
    sys.stderr.write('class name is %s\n' % class_name)
    m = __import__(module)
    sys.stderr.write('loaded  module %s\n' % str(m))
    for part in parts[1:]:
        m = getattr(m, part)
        sys.stderr.write('loaded  module %s\n' % str(m))

    return m


class HBaseProtocol(object):
    HBASE_TABLE_ENV = 'mrjob_hbase_table'
    HBASE_SERVER_ENV = 'mrjob_hbase_server'
    HBASE_COLUMN_FAMILY_ENV = 'mrjob_hbase_cf'

    DEFAULT_BATCH_SIZE = 1000

    def __init__(self):

        if not self.HBASE_TABLE_ENV in os.environ:
            raise ValueError('Must specify hbase column to write to')
        else:
            table_name = os.environ[self.HBASE_TABLE_ENV]

        if self.HBASE_COLUMN_FAMILY_ENV in os.environ:
            cf = os.environ[self.HBASE_COLUMN_FAMILY_ENV]
        else:
            cf = None

        if not self.HBASE_SERVER_ENV in os.environ:
            raise ValueError('Must specify hbase servers to write to')
        else:
            servers_str = os.environ[self.HBASE_SERVER_ENV]
            servers = servers_str.split(',')

        from hbase import Exporter
        self.writer = Exporter(servers, table_name, col_family=cf, batch_size=HBaseProtocol.DEFAULT_BATCH_SIZE)

    def write(self, key, value):
        self.writer.put(key, value)
        # for the sake of normal writer flow, it is assumed this output is redirected to a temp dir
        return ''


class TextProtocol(object):

    SEPARATOR = ''

    def read(self, line):
        return line

    def write(self, key, value):
        return key + TextProtocol.SEPARATOR + value


class TsvProtocol(object):
    TAB_SEPARATOR = '\t'

    KEY_CLASS_PROPERTY_ENV = 'key_class_name'
    VALUE_CLASS_PROPERTY_ENV = 'value_class_name'

    def __init__(self):
        self.instances = dict()
        self.file_name_2_key_class = dict()
        self.file_name_2_value_class = dict()

    @staticmethod
    def named_key_class_env(name):
        return encode_env('%s_%s' % (TsvProtocol.KEY_CLASS_PROPERTY_ENV, name))

    @staticmethod
    def named_value_class_env(name):
        return encode_env('%s_%s' % (TsvProtocol.VALUE_CLASS_PROPERTY_ENV, name))

    def determine_key_class(self):
        file_name = os.environ['map_input_file']
        try_find = self.file_name_2_key_class.get(file_name, None)
        if try_find is not None:
            return try_find

        for env_key in os.environ:
            reverted = decode_env(env_key)
            if reverted is None:
                continue

            if TsvProtocol.KEY_CLASS_PROPERTY_ENV in reverted and reverted[len(
                    TsvProtocol.KEY_CLASS_PROPERTY_ENV + '_'):] in file_name:
                self.file_name_2_key_class[file_name] = os.environ[env_key]
                return os.environ[env_key]

        sys.stderr.write('key class undefined for %s\n' % file_name)
        raise Exception('key class undefined for %s' % file_name)

    def determine_value_class(self):
        file_name = os.environ['map_input_file']
        try_find = self.file_name_2_value_class.get(file_name, None)
        if try_find is not None:
            return try_find

        for env_key in os.environ:
            reverted = decode_env(env_key)
            if reverted is None:
                continue

            if TsvProtocol.VALUE_CLASS_PROPERTY_ENV in reverted and reverted[len(
                    TsvProtocol.VALUE_CLASS_PROPERTY_ENV + '_'):] in file_name:
                self.file_name_2_value_class[file_name] = os.environ[env_key]
                return os.environ[env_key]

        sys.stderr.write('value class undefined for %s\n' % file_name)
        raise Exception('value class undefined for %s' % file_name)

    def get_instance(self, cls):
        existing = self.instances.get(cls, None)
        if existing is not None:
            existing.clear()
            return existing
        else:
            new_instance = load_class(cls)()
            self.instances[cls] = new_instance
            return new_instance

    @staticmethod
    def read_value(object, fields, idx):
        return object.read_tsv(fields, idx)

    @staticmethod
    def read_dict(key_class, value_class, fields, idx):
        ret = {}
        ret_idx = idx

        dict_len = int(fields[ret_idx])
        ret_idx += 1

        for i in range(dict_len):
            key = key_class()
            ret_idx = TsvProtocol.read_value(key, fields, ret_idx)
              
            value = value_class()
            ret_idx = TsvProtocol.read_value(value, fields, ret_idx)
 
            ret[key] = value

        return ret, ret_idx

    @staticmethod
    def read_list(element_class, fields, idx):
        ret = []
        ret_idx = idx

        list_len = int(fields[ret_idx])
        ret_idx += 1

        for i in range(list_len):
            elem = element_class()
            ret_idx = TsvProtocol.read_value(elem, fields, ret_idx)

            ret += [elem]

        return ret, ret_idx
 
    @staticmethod
    def read_tuple(element_classes, fields, idx):
        lst = []
        ret_idx = idx

        list_len = int(fields[ret_idx])
        ret_idx += 1

        for i in range(list_len):
            elem = element_classes[i]()
            ret_idx = TsvProtocol.read_value(elem, fields, ret_idx)

            lst += [elem]

        return tuple(lst), ret_idx   

    def read(self, line):
        fields = line.split(TsvProtocol.TAB_SEPARATOR)
        idx = 1 if 'combine' in os.environ['mapred_input_format_class'].lower() else 0

        key = self.get_instance(self.determine_key_class())
        idx = TsvProtocol.read_value(key, fields, idx)
        value = self.get_instance(self.determine_value_class())
        TsvProtocol.read_value(value, fields, idx)

        return key, value

    @staticmethod
    def parse_and_write(obj):

        ret = ''

        if isinstance(obj, dict):

            ret += str(len(obj))

            for key in obj:
                ret += TsvProtocol.TAB_SEPARATOR
                ret += TsvProtocol.parse_and_write(key)
                ret += TsvProtocol.TAB_SEPARATOR
                ret += TsvProtocol.parse_and_write(obj[key])

        elif isinstance(obj, list):
            ret += str(len(obj))
            for element in obj:
                ret += TsvProtocol.TAB_SEPARATOR
                ret += TsvProtocol.parse_and_write(element)

        elif isinstance(obj, tuple):
            ret += TsvProtocol.TAB_SEPARATOR.join([TsvProtocol.parse_and_write(elem) for elem in obj])

        else:
            ret += obj.to_tsv()

        return ret

    def write(self, key, value):
        return self.parse_and_write(key) + self.TAB_SEPARATOR + self.parse_and_write(value)
