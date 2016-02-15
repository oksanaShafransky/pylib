__author__ = 'Felix'

from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults

from airflow_etcd import EtcdKeyValueProvider
from airflow_consul import ConsulKeyValueProvider
PROVIDERS = [ConsulKeyValueProvider, EtcdKeyValueProvider]


class AggregateOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(AggregateOperator, self).__init__(*args, **kwargs)
        kwargs.pop('dag')
        self.operators = [oper()(owner='nobody', *args, **kwargs) for oper in self.get_operators()]
        self.store_template_fields()

    # please override this in each inheriting class, this is defined only for clarity
    def get_action(self):
        pass

    def get_operators(self):
        return [getattr(provider, self.get_action()) for provider in PROVIDERS]

    def store_template_fields(self):
        self.template_store = dict()
        all_template_fields = set(self.template_fields or [])
        for sub_operator in self.operators:
            self.template_store[sub_operator] = sub_operator.template_fields
            for named_template_field in sub_operator.template_fields:
                setattr(self, named_template_field, getattr(sub_operator, named_template_field))
                all_template_fields.add(named_template_field)

        setattr(self.__class__, 'template_fields', all_template_fields)

    def assign_template_fields(self):
        for sub_operator in self.operators:
            operator_template_fields = self.template_store[sub_operator]
            for template_field in operator_template_fields:
                setattr(sub_operator, template_field, getattr(self, template_field, None))

    def execute(self, context):
        self.assign_template_fields()
        for sub_operator in self.operators:
            sub_operator.execute(context)


class KeyValueSetOperator(AggregateOperator):
    ui_color = '#00BFFF'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(KeyValueSetOperator, self).__init__(*args, **kwargs)

    @staticmethod
    def get_action():
        return 'setter'


class KeyValuePromoteOperator(AggregateOperator):
    ui_color = '#00BFFF'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(KeyValuePromoteOperator, self).__init__(*args, **kwargs)

    @staticmethod
    def get_action():
        return 'promoter'


class KeyValueDeleteOperator(AggregateOperator):
    ui_color = '#00BFFF'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(KeyValueDeleteOperator, self).__init__(*args, **kwargs)

    @staticmethod
    def get_action():
        return 'eraser'


class AggregateSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(AggregateSensor, self).__init__(*args, **kwargs)
        kwargs.pop('dag')
        self.operators = [oper()(owner='nobody', *args, **kwargs) for oper in self.get_operators()]
        self.store_template_fields()

    # please override this in each inheriting class, this is defined only for clarity
    def get_action(self):
        pass

    def get_operators(self):
        return [getattr(provider, self.get_action()) for provider in PROVIDERS]

    def store_template_fields(self):
        self.template_store = dict()
        all_template_fields = set(self.template_fields or [])
        for sub_operator in self.operators:
            self.template_store[sub_operator] = sub_operator.template_fields
            for named_template_field in sub_operator.template_fields:
                setattr(self, named_template_field, getattr(sub_operator, named_template_field))
                all_template_fields.add(named_template_field)

        setattr(self.__class__, 'template_fields', all_template_fields)

    def assign_template_fields(self):
        for sub_operator in self.operators:
            operator_template_fields = self.template_store[sub_operator]
            for template_field in operator_template_fields:
                setattr(sub_operator, template_field, getattr(self, template_field, None))

    def poke(self, context):
        self.assign_template_fields()
        for sub_operator in self.operators:
            try:
                if sub_operator.poke(context):
                    return True
            except:
                pass

        return False


class KeyValueSensor(AggregateSensor):
    ui_color = '#00BFFF'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(KeyValueSensor, self).__init__(*args, **kwargs)

    @staticmethod
    def get_action():
        return 'sensor'


# this sensor fetches a list of keys under a given key, then polls each member under some base key and compares to a desired value
class KeyValueCompoundSensor(AggregateSensor):
    ui_color = '#00BFFF'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(KeyValueCompoundSensor, self).__init__(*args, **kwargs)

    @staticmethod
    def get_action():
        return 'compound_sensor'


class KeyValueCompoundDateSensor(AggregateSensor):
    ui_color = '#00BFFF'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(KeyValueCompoundDateSensor, self).__init__(*args, **kwargs)

    @staticmethod
    def get_action():
        return 'compound_date_sensor'