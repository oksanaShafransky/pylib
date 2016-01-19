__author__ = 'Felix'

import logging
from etcd.client import Client
from datetime import datetime

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults
from airflow.plugins_manager import AirflowPlugin


ETCD_ENV_ROOT = {'STAGING': 'v1/staging', 'PRODUCTION': 'v1/production', 'DEFAULT': 'v1'}


class EtcdHook(BaseHook):
    '''
    Interact with ETCD. This class is a wrapper around the etcd library.
    '''
    def __init__(self, etcd_conn_id='etcd_default'):
        self.etcd_conn_id = etcd_conn_id

    def get_conn(self):
        connections = self.get_connections(self.etcd_conn_id)
        client = Client(connections[0].host, connections[0].port)
        return client

    def get_record(self, root, path):
        full_path = '/%s/%s' % (root, path)
        client = self.get_conn()
        return client.get(full_path).value


class EtcdOperator(BaseOperator):
    ui_color = '#00BFFF'
    template_fields = ('env', 'path')

    @apply_defaults
    def __init__(self, etcd_conn_id='etcd_default', path='', env='DEFAULT', *args, **kwargs):
        super(EtcdOperator, self).__init__(*args, **kwargs)
        self.etcd_conn_id = etcd_conn_id
        self.env = env
        self.path = path

    def get_path(self):
        return '/%s/%s' % (ETCD_ENV_ROOT[self.env], self.path)

    def get_client(self):
        return EtcdHook(self.etcd_conn_id).get_conn()


class EtcdSetOperator(EtcdOperator):
    template_fields = ('env', 'path', 'value')

    @apply_defaults
    def __init__(self, value='success', *args, **kwargs):
        super(EtcdSetOperator, self).__init__(*args, **kwargs)
        self.value = value

    def execute(self, context):
        path = self.get_path()
        logging.info('etcd path is %s, value is %s' % (path, self.value))
        self.get_client().set(str(path), str(self.value))


class EtcdPromoteOperator(EtcdOperator):
    # sets the given value only if it is greater than the existing value at the key
    template_fields = ('env', 'path', 'value')

    @apply_defaults
    def __init__(self, value='success', value_parser=lambda x: x, *args, **kwargs):
        super(EtcdPromoteOperator, self).__init__(*args, **kwargs)
        self.value = value
        self.parser = value_parser

    def execute(self, context):
        logging.info('etcd path is %s, value is %s' % (self.path, self.value))
        try:
            curr = self.get_client().get(str(self.path))
            curr_value = self.parser(curr.value)
            logging.info('retrieved existing value %s' % str(curr_value))
            if self.value > curr_value:
                self.get_client().set(str(self.get_path()), str(self.value))
        except Exception:
            self.get_client().set(str(self.get_path()), str(self.value))


class EtcdDeleteOperator(EtcdOperator):
    template_fields = ('env', 'path')

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(EtcdDeleteOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        path = self.get_path()
        logging.info('etcd path to delete is %s' % path)
        try:
            self.get_client().delete(str(path))
        except Exception:
            logging.info('key was not found')
            # exit gracefully


def test_etcd_value(client, path, criteria):
    res = client.get(path)
    return criteria(res.value)


class EtcdSensor(BaseSensorOperator):
    # Pass desired_value as None if you wish to merely make sure a key exists

    ui_color = '#00BFFF'
    template_fields = ('env', 'path')

    @apply_defaults
    def __init__(self, etcd_conn_id='etcd_default', path='', desired_value='success', env='DEFAULT', *args, **kwargs):
        super(EtcdSensor, self).__init__(*args, **kwargs)
        self.etcd_conn_id = etcd_conn_id
        self.env = env
        self.path = path
        if hasattr(desired_value, '__call__'):
            self.cmp_criteria = desired_value
        elif desired_value is not None:
            self.cmp_criteria = lambda x: x == desired_value
        else:
            self.cmp_criteria = lambda x: True

    def get_client(self):
        return EtcdHook(self.etcd_conn_id).get_conn()

    def get_path(self):
        return '/%s/%s' % (ETCD_ENV_ROOT[self.env], self.path)

    def poke(self, context):
        path = self.get_path()
        logging.info('testing etcd path %s' % path)
        try:
            return test_etcd_value(self.get_client(), str(path), self.cmp_criteria)
        except Exception:
            # this means the key is not present
            return False


# this sensor fetches a list of keys under a given key, then polls each member under some base key and compares to a desired value
class CompoundEtcdSensor(EtcdSensor):

    # Pass desired_value as None if you wish to merely make sure a key exists
    template_fields = ('env', 'key_list_path', 'key_root', 'list_separator')

    @apply_defaults
    def __init__(self, key_list_path='', list_separator=',', key_root='', key_suffix='', env='DEFAULT', *args, **kwargs):
        super(CompoundEtcdSensor, self).__init__(*args, **kwargs)
        self.key_list_path = key_list_path
        self.list_separator = list_separator
        self.env = env
        self.key_root = key_root
        self.key_suffix = key_suffix

    def get_check_keys_path(self):
        return '/%s/%s' % (ETCD_ENV_ROOT[self.env], self.key_list_path)

    def get_subkeys_base(self):
        return '/%s/%s' % (ETCD_ENV_ROOT[self.env], self.key_root)

    def poke(self, context):
        try:
            key_list_path = self.get_check_keys_path()
            logging.info('fetching key list from etcd path %s' % key_list_path)
            val_str = self.get_client().get(str(key_list_path)).value
            keys_to_check = val_str.split(self.list_separator)
        except Exception as e:
            logging.error('key list path not found')
            raise e

        try:
            key_root = self.get_subkeys_base()
            for key in keys_to_check:
                key_path = str(key_root + '/' + key + self.key_suffix)
                logging.info('testing path %s' % key_path)
                if not test_etcd_value(self.get_client(), key_path, self.test_value):
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
    template_fields = ('env', 'key_list_path', 'key_root', 'list_separator', 'desired_date')

    @apply_defaults
    def __init__(self, desired_date, *args, **kwargs):
        super(CompoundDateEtcdSensor, self).__init__(*args, **kwargs)
        self.desired_date = desired_date

    def test_value(self, dt):
        return datetime.strptime(dt, '%Y-%m-%d') >= datetime.strptime(self.desired_date, '%Y-%m-%d')


class SWEtcdAirflowPluginManager(AirflowPlugin):

    name = 'SWEtcdOperators'

    operators = [EtcdSetOperator, EtcdPromoteOperator, EtcdDeleteOperator, EtcdSensor, CompoundEtcdSensor, CompoundDateEtcdSensor]


class EtcdKeyValueProvider:
    def __init__(self):
        pass

    @staticmethod
    def setter():
        return EtcdSetOperator

    @staticmethod
    def promoter():
        return EtcdPromoteOperator

    @staticmethod
    def eraser():
        return EtcdDeleteOperator

    @staticmethod
    def sensor():
        return EtcdSensor

    @staticmethod
    def compound_sensor():
        return CompoundEtcdSensor

    @staticmethod
    def compound_date_sensor():
        return CompoundDateEtcdSensor
