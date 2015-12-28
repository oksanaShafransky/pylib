__author__ = 'Felix'

import logging
from etcd.client import Client
from datetime import datetime

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults
from airflow.plugins_manager import AirflowPlugin


class EtcdHook(BaseHook):
    '''
    Interact with ETCD. This class is a wrapper around the etcd library.
    '''
    def __init__(self, etcd_conn_id='etcd_default'):
        self.etcd_conn_id = etcd_conn_id

    def get_conn(self):
        connections = self.get_connections(self.etcd_conn_id)
        client = Client(tuple((conn.host, conn.port) for conn in connections))
        return client

    def get_record(self, root, path):
        full_path = '/%s/%s' % (root, path)
        client = self.get_conn()
        return client.get(full_path).value


class EtcdSetOperator(BaseOperator):
    ui_color = '#00BFFF'
    template_fields = ('path', 'value')

    @apply_defaults
    def __init__(self, etcd_conn_id='etcd_default', path='', value='success', root='v1', *args, **kwargs):
        super(EtcdSetOperator, self).__init__(*args, **kwargs)

        self.client = EtcdHook(etcd_conn_id).get_conn()
        self.path = '/%s/%s' % (root, path)
        self.value = value

    def execute(self, context):
        logging.info('etcd path is %s, value is %s' % (self.path, self.value))
        self.client.set(str(self.path), str(self.value))


def test_etcd_value(client, path, criteria):
    res = client.get(path)
    return criteria(res.value)


class EtcdSensor(BaseSensorOperator):

    # Pass desired_value as None if you wish to merely make sure a key exists

    ui_color = '#00BFFF'
    template_fields = ('path',)

    @apply_defaults
    def __init__(self, etcd_conn_id='etcd_default', path='', desired_value='success', root='v1', *args, **kwargs):
        super(EtcdSensor, self).__init__(*args, **kwargs)
        self.client = EtcdHook(etcd_conn_id).get_conn()
        self.path = '/%s/%s' % (root, path)
        if hasattr(desired_value, '__call__'):
            self.cmp_criteria = desired_value
        elif desired_value is not None:
            self.cmp_criteria = lambda x: x == desired_value
        else:
            self.cmp_criteria = lambda x: True

    def poke(self, context):
        logging.info('testing etcd path %s' % self.path)
        try:
            return test_etcd_value(self.client, str(self.path), self.cmp_criteria)
        except Exception:
            # this means the key is not present
            return False


# this sensor fetches a list of keys under a given key, then polls each member under some base key and compares to a desired value
class CompoundEtcdSensor(BaseSensorOperator):

    # Pass desired_value as None if you wish to merely make sure a key exists

    ui_color = '#00BFFF'
    template_fields = ('key_list_path', 'key_root', 'list_separator')

    @apply_defaults
    def __init__(self, etcd_conn_id='etcd_default', key_list_path='', list_separator=',', key_root='', key_suffix='', desired_value='success', root='v1', *args, **kwargs):
        super(CompoundEtcdSensor, self).__init__(*args, **kwargs)
        self.client = EtcdHook(etcd_conn_id).get_conn()
        self.key_list_path = '/%s/%s' % (root, key_list_path)
        self.list_separator = list_separator
        self.key_root = '/%s/%s' % (root, key_root)
        self.key_suffix = key_suffix
        if hasattr(desired_value, '__call__'):
            self.cmp_criteria = desired_value
        elif desired_value is not None:
            self.cmp_criteria = lambda x: x == desired_value
        else:
            self.cmp_criteria = lambda x: True

    def poke(self, context):
        try:
            logging.info('fetching key list from etcd path %s' % self.key_list_path)
            val_str = self.client.get(str(self.key_list_path)).value
            keys_to_check = val_str.split(self.list_separator)
        except Exception as e:
            logging.error('key list path not found')
            raise e

        try:
            for key in keys_to_check:
                key_path = str(self.key_root + '/' + key + self.key_suffix)
                logging.info('testing path %s' % key_path)
                if not test_etcd_value(self.client, key_path, self.test_value):
                    logging.info('not ready')
                    return False

            logging.info('all passed')
            return True

        except Exception as e:
            logging.error(e)
            # this means the key is not present
            return False

    def test_value(self, val):
        return self.cmp_criteria(val)


class CompoundDateEtcdSensor(CompoundEtcdSensor):
    ui_color = '#00BFFF'
    template_fields = ('key_list_path', 'key_root', 'list_separator', 'desired_date')

    @apply_defaults
    def __init__(self, desired_date, *args, **kwargs):
        super(CompoundDateEtcdSensor, self).__init__(*args, **kwargs)
        self.cnt = 0
        self.desired_date = desired_date

    def test_value(self, dt):
        return datetime.strptime(dt, '%Y-%m-%d') >= datetime.strptime(self.desired_date, '%Y-%m-%d')


class EtcdDeleteOperator(BaseOperator):
    ui_color = '#00BFFF'
    template_fields = ('path',)

    @apply_defaults
    def __init__(self, etcd_conn_id='etcd_default', path='', root='v1', *args, **kwargs):
        super(EtcdDeleteOperator, self).__init__(*args, **kwargs)
        self.client = EtcdHook(etcd_conn_id).get_conn()
        self.path = '/%s/%s' % (root, path)

    def execute(self, context):
        logging.info('etcd path to delete is %s' % self.path)
        try:
            self.client.delete(str(self.path))
        except Exception:
            logging.info('key was not found')
            # exit gracefully


class EtcdPromoteOperator(BaseOperator):
    # sets the given value only if it is greater than the existing value at the key

    ui_color = '#00BFFF'
    template_fields = ('path', 'value')

    @apply_defaults
    def __init__(self, etcd_conn_id='etcd_default', path='', value='success', root='v1', value_parser=lambda x: x, *args, **kwargs):
        super(EtcdPromoteOperator, self).__init__(*args, **kwargs)
        self.client = EtcdHook(etcd_conn_id).get_conn()
        self.path = '/%s/%s' % (root, path)
        self.value = value
        self.parser = value_parser

    def execute(self, context):
        logging.info('etcd path is %s, value is %s' % (self.path, self.value))
        try:
            curr = self.client.get(str(self.path))
            curr_value = self.parser(curr.value)
            logging.info('retrieved existing value %s' % str(curr_value))
            if self.value > curr_value:
                self.client.set(str(self.path), str(self.value))
        except Exception:
            self.client.set(str(self.path), str(self.value))


class SWEtcdAirflowPluginManager(AirflowPlugin):

    name = 'SWEtcdOperators'

    operators = [EtcdSetOperator, EtcdPromoteOperator, EtcdDeleteOperator, EtcdSensor]
