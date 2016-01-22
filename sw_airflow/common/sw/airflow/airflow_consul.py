__author__ = 'Felix'

import logging
import consulate
from datetime import datetime

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults
from airflow.plugins_manager import AirflowPlugin


# Consul Wrapper for airflow
class ConsulHook(BaseHook):
    def __init__(self, consul_env=None):
        self.env = consul_env
        self._client = None

    @property
    def client(self):
        return self._client or self._init_client()

    def _init_client(self):
        self._client = consulate.Consul('consul.service%(suffix)s' % {'suffix': ('.' + self.env if self.env else '')})
        return self._client

    @staticmethod
    def normalize_path(path):
        return path.lstrip('/').replace('//', '/')

    def get_record(self, path):
        return self.client.kv.get(self.normalize_path(path))


class ConsulOperator(BaseOperator):
    ui_collor = '#ff64ff'
    template_fields = ('env', 'path')

    @apply_defaults
    def __init__(self, path='', env=None, *args, **kwargs):
        super(ConsulOperator, self).__init__(*args, **kwargs)
        self.env = env
        self.path = path
        self._conn = None

    @property
    def client(self):
        return (self._conn or self._init_conn()).client

    def _init_conn(self):
        self._conn = ConsulHook(consul_env=self.env)
        return self._conn


class ConsulSetOperator(ConsulOperator):
    template_fields = ('env', 'path', 'value')

    @apply_defaults
    def __init__(self, value='success', *args, **kwargs):
        super(ConsulSetOperator, self).__init__(*args, **kwargs)
        self.value = value

    def execute(self, context):
        logging.info('consul: writing %s to %s' % (self.value, self.path))
        self.client.kv.set(str(self.path), self.value)


class ConsulPromoteOperator(ConsulOperator):
    # sets the given value only if it is greater than the existing value at the key
    template_fields = ('env', 'path', 'value')

    @apply_defaults
    def __init__(self, value='success', value_parser=lambda x: x, *args, **kwargs):
        super(ConsulPromoteOperator, self).__init__(*args, **kwargs)
        self.value = value
        self.parser = value_parser

    def execute(self, context):
        logging.info('consul: checking value at %s' % self.path)
        try:
            curr = self.client.kv.get(str(self.path))
            curr_value = self.parser(curr)
            logging.info('retrieved existing value %s' % str(curr_value))
            if self.value > curr_value:
                logging.info('updating value to %s' % self.value)
                self.client.kv.set(str(self.path), str(self.value))
            else:
                logging.info('existing value is greater than %s' % self.value)
        except Exception:
            self.client.kv.set(str(self.path), str(self.value))


class ConsulDeleteOperator(ConsulOperator):
    template_fields = ('env', 'path')

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ConsulDeleteOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        logging.info('etcd path to delete is %s' % self.path)
        try:
            self.client.delete(str(self.path))
        except Exception:
            logging.info('key was not found')
            # exit gracefully


class ConsulSensor(BaseSensorOperator):
    # Pass desired_value as None if you wish to merely make sure a key exists

    ui_collor = '#ff64ff'
    template_fields = ('env', 'path')

    @apply_defaults
    def __init__(self, path='', env=None, desired_value='success', *args, **kwargs):
        super(ConsulSensor, self).__init__(*args, **kwargs)
        self.env = env
        self.path = path
        self._conn = None

        if hasattr(desired_value, '__call__'):
            self.cmp_criteria = desired_value
        elif desired_value is not None:
            self.cmp_criteria = lambda x: x == desired_value
        else:
            self.cmp_criteria = lambda x: True

    @property
    def client(self):
        return (self._conn or self._init_conn()).client

    def _init_conn(self):
        self._conn = ConsulHook(consul_env=self.env)
        return self._conn

    def test_value(self, path):
        val = self.client.kv.get(path)
        return self.cmp_criteria(val)

    def poke(self, context):
        logging.info('testing consul path %s' % self.path)
        try:
            return self.test_value(self.path)
        except Exception:
            # this means the key is not present
            return False


class CompoundConsulSensor(ConsulSensor):

    # Pass desired_value as None if you wish to merely make sure a key exists
    @apply_defaults
    def __init__(self, key_list_path='', list_separator=',', key_root='', key_suffix='', *args, **kwargs):
        super(CompoundConsulSensor, self).__init__(*args, **kwargs)
        self.key_list_path = key_list_path
        self.list_separator = list_separator
        self.key_root = key_root
        self.key_suffix = key_suffix

    def poke(self, context):
        try:
            logging.info('consul: fetching key list from path %s' % self.key_list_path)
            val_str = self.client.kv.get(str(self.key_list_path))
            keys_to_check = val_str.split(self.list_separator)
        except Exception as e:
            logging.error('key list path not found')
            raise e

        try:
            for key in keys_to_check:
                key_path = str(self.key_root + '/' + key + self.key_suffix)
                logging.info('testing path %s' % key_path)
                if not self.test_value(key_path):
                    logging.info('not ready')
                    return False

            logging.info('all passed')
            return True

        except Exception as e:
            logging.error(e)
            # this means the key is not present
            return False


class CompoundDateConsulSensor(CompoundConsulSensor):
    template_fields = ('env', 'key_list_path', 'key_root', 'list_separator', 'desired_date')

    @apply_defaults
    def __init__(self, desired_date, *args, **kwargs):
        super(CompoundDateConsulSensor, self).__init__(*args, **kwargs)
        self.desired_date = desired_date

    def cmp_criteria(self, dt):
        logging.info('comparing %s to %s' % (dt, self.desired_date))
        return datetime.strptime(dt, '%Y-%m-%d') >= datetime.strptime(self.desired_date, '%Y-%m-%d')


class SWConsulAirflowPluginManager(AirflowPlugin):

    name = 'SWConsulOperators'

    operators = [ConsulSetOperator, ConsulPromoteOperator, ConsulDeleteOperator, ConsulSensor, CompoundConsulSensor, CompoundDateConsulSensor]


class ConsulKeyValueProvider:
    def __init__(self):
        pass

    @staticmethod
    def setter():
        return ConsulSetOperator

    @staticmethod
    def promoter():
        return ConsulPromoteOperator

    @staticmethod
    def eraser():
        return ConsulDeleteOperator

    @staticmethod
    def sensor():
        return ConsulSensor

    @staticmethod
    def compound_sensor():
        return CompoundConsulSensor

    @staticmethod
    def compound_date_sensor():
        return CompoundDateConsulSensor