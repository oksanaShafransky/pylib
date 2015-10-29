__author__ = 'Felix'

import logging
from etcd.client import Client

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
        client = Client([((conn.host, conn.port) for conn in connections)])
        return client


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


class EtcdSensor(BaseSensorOperator):

    # Pass desired_value as None if you wish to merely make sure a key exists

    ui_color = '#00BFFF'
    template_fields = ('path', 'desired_value')

    @apply_defaults
    def __init__(self, etcd_conn_id='etcd_default', path='', desired_value='success', root='v1', *args, **kwargs):
        super(EtcdSensor, self).__init__(*args, **kwargs)
        self.client = EtcdHook(etcd_conn_id).get_conn()
        self.path = '/%s/%s' % (root, path)
        self.desired_value = desired_value

    def poke(self, context):
        logging.info('testing etcd path %s' % self.path)
        try:
            val = self.client.get(str(self.path))
            return True if self.desired_value is None else val.value == self.desired_value
        except Exception:
            # this means the key is not present
            return False


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
