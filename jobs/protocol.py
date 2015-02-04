import binascii

__author__ = 'Felix'

import os
import sys
import binascii
from hbase import Exporter
import xml.etree.ElementTree as ET


def encode_env(s):
    return 'v' + binascii.b2a_hex(s)


def decode_env(s):
    if s[0] == 'v':
        return binascii.a2b_hex(s[1:])


def load_class(class_name):
    parts = class_name.split('.')
    module = ".".join(parts[:-1])
    m = __import__( module )
    for comp in parts[1:]:
        m = getattr(m, comp)            
    return m


def determine_hbase_servers():
        hbase_conf = os.environ['HBASE_CONF_DIR'] if 'HBASE_CONF_DIR' in os.environ else '/etc/hbase/conf'

        conf = ET.parse('%s/hbase-site.xml' % hbase_conf)
        root = conf.getroot()

        # should only be 1
        quorum_prop = [elem.find('value').text for elem in root.findall('property') if elem.find('name').text == 'hbase.zookeeper.quorum'][0]
        return quorum_prop.split(',')


class HBaseProtocol(object):
    HBASE_TABLE_ENV = 'mrjob_hbase_table'
    HBASE_COLUMN_FAMILY_ENV = 'mrjob_hbase_cf'

    def __init__(self):

        if not self.HBASE_TABLE_ENV in os.environ:
            raise 'Must specify hbase column to write to'
        else:
            table_name = os.environ[self.HBASE_TABLE_ENV]

        if self.HBASE_COLUMN_FAMILY_ENV in os.environ:
            cf = os.environ[self.HBASE_COLUMN_FAMILY_ENV]

        for server in determine_hbase_servers():
            try:
                self.writer = Exporter(server, table_name, col_family=cf)
                if self.writer:
                    break
            except:
                continue


    def write(self, key, value):
        self.writer.put(key, value)


class TsvProtocol(object):
    TAB_SEPARATOR = '\t'

    KEY_CLASS_PROPERTY_ENV = 'key_class_name'
    VALUE_CLASS_PROPERTY_ENV = 'value_class_name'

    @staticmethod
    def named_key_class_env(name):
        return encode_env('%s_%s' % (TsvProtocol.KEY_CLASS_PROPERTY_ENV, name))

    @staticmethod
    def named_value_class_env(name):
        return encode_env('%s_%s' % (TsvProtocol.VALUE_CLASS_PROPERTY_ENV, name))

    @staticmethod
    def determine_key_class():
        file_name = os.environ['map_input_file']

        for env_key in os.environ:
            reverted = decode_env(env_key)
            if reverted is None:
                continue

            if TsvProtocol.KEY_CLASS_PROPERTY_ENV in reverted and reverted[len(
                    TsvProtocol.KEY_CLASS_PROPERTY_ENV + '_'):] in file_name:
                return load_class(os.environ[env_key])

        sys.stderr.write('key class undefined for %s\n' % file_name)
        raise Exception('key class undefined for %s' % file_name)

    @staticmethod
    def determine_value_class():
        file_name = os.environ['map_input_file']
        for env in os.environ:
            reverted = decode_env(env)
            if reverted is None:
                continue

            if TsvProtocol.VALUE_CLASS_PROPERTY_ENV in reverted and reverted[len(
                    TsvProtocol.VALUE_CLASS_PROPERTY_ENV + '_'):] in file_name:
                return load_class(os.environ[env])

        sys.stderr.write('value class undefined for %s\n' % file_name)
        raise Exception('value class undefined for %s' % file_name)

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
        idx = 0

        key = TsvProtocol.determine_key_class()()
        idx = TsvProtocol.read_value(key, fields, idx)

        value = TsvProtocol.determine_value_class()()
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
