__author__ = 'Felix'

import sys
import os
from hbase import Exporter
import xml.etree.ElementTree as ET


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

    def parseAndWrite(self, obj):

        ret = ''

        if isinstance(obj, dict):

            ret += str(len(obj))

            for key in obj:
                ret += self.TAB_SEPARATOR
                ret += self.parseAndWrite(key)
                ret += self.TAB_SEPARATOR
                ret += self.parseAndWrite(obj[key])

        elif isinstance(obj, list):
            ret += str(len(obj))
            for element in obj:
                ret += self.TAB_SEPARATOR
                ret += self.parseAndWrite(element)

        elif isinstance(obj, tuple):
            ret += self.TAB_SEPARATOR.join([self.parseAndWrite(elem) for elem in obj])

        else:
            ret += str(obj)

        return ret

    def write(self, key, value):
        return self.parseAndWrite(key) + self.TAB_SEPARATOR + self.parseAndWrite(value)