# ! /usr/bin/env python

import argparse
from datetime import datetime

CONCURRENCY = 6


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return datetime.strptime(date_str, '%Y-%m')


class Arg(object):
    @staticmethod
    def date_arg(date_str):
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return datetime.strptime(date_str, '%Y-%m')

    @staticmethod
    def list_arg(arg_str):
        return arg_str.split(',')

    def __init__(self, short_name, long_name, attribute, type, help, required=True, default=None):
        self.short = short_name
        self.long = long_name
        self.attribute = attribute

        self.registered = False

        if isinstance(type, tuple):
            self.type = 'choice'
            self.choices = type
        else:
            self.type = type

        self.help = help

        self.required = required
        self.default_value = default

    def add_argument(self, target_parser):
        if self.type == 'choice':
            target_parser.add_argument(self.short, self.long, choices=self.choices, dest=self.attribute,
                                       required=self.required, default=self.default_value, help=self.help)
        elif self.type == bool:
            target_parser.add_argument(self.short, self.long, action='store_true', dest=self.attribute,
                                       required=self.required, default=self.default_value, help=self.help)
        elif type == str:
            target_parser.add_argument(self.short, self.long, dest=self.attribute, required=self.required,
                                       default=self.default_value, help=self.help)
        else:
            target_parser.add_argument(self.short, self.long, type=self.type, dest=self.attribute,
                                       required=self.required,
                                       default=self.default_value, help=self.help)

        self.registered = True


class Const(object):
    def __init__(self, value):
        self.value = value


class Action(object):
    def __init__(self, action_name, action_params, group, kw_params=None, parent_parser=None, action_help=None):

        self.name = action_name
        self.params = action_params
        if kw_params:
            self.kw_params = kw_params
        else:
            self.kw_params = {}

        self.help = action_help

        self.generate_parser(group, parent_parser)

    def generate_parser(self, parsers, parent_parser=None):
        parent_parsers = [parent_parser] if parent_parser else []
        action_parser = parsers.add_parser(self.name, help=self.help, parents=parent_parsers)

        for param in (self.params + self.kw_params.values()):
            if isinstance(param, Arg) and not param.registered:
                param.add_argument(action_parser)


class Stage(object):
    def __init__(self, name, queries):
        self.name = name
        self.queries = queries


def upsert_param(params, new_param):
    """ Replace param if it is already in common params list with new definition. Check is done by target attribute"""
    candidates = [param for param in params if param.attribute == new_param.attribute]

    if len(candidates) not in [0, 1]:
        raise argparse.ArgumentError(argument=None, message='colliding params: ' + str(candidates))

    if len(candidates) == 1:
        params.remove(candidates[0])

    return params + [new_param]


class Executer(object):
    def __init__(self):
        self.actions = {}
        self.base_parser = argparse.ArgumentParser('executer.py')
        self.subparsers = self.base_parser.add_subparsers(dest='action')

        self.common_params = self.get_common_params()
        self.common_parser = argparse.ArgumentParser(add_help=False)

        for common_param in self.common_params:
            common_param.add_argument(self.common_parser)

    # set common_params in subclasses to use for all their actions
    def get_common_params(self):
        return []

    # define default values for arguments depending on values of others
    def get_arg_dependencies(self):
        return {}

    def common_param(self, param_name):
        for common_param in self.common_params:
            if common_param.attribute == param_name:
                return common_param

    def add_action(self, action_name, action_handler, action_params, kw_params=None, help=None):
        action = Action(action_name, action_params, self.subparsers, kw_params=kw_params,
                        parent_parser=self.common_parser, action_help=help)
        self.add_stage(action_name, [(action_handler, action)])

    def add_stage(self, stage_name, handler_action_list):
        self.actions[stage_name] = handler_action_list

    def execute(self):

        self.args = self.base_parser.parse_args()

        dependencies = self.get_arg_dependencies()
        for (arg, val) in dependencies:
            if getattr(self.args, arg) == val:
                dep_arg, dep_val = dependencies[(arg, val)]
                if not hasattr(self.args, dep_arg) or not getattr(self.args, dep_arg):
                    setattr(self.args, dep_arg, dep_val)

        action_name = self.args.action

        if action_name not in self.actions:
            self.common_parser.error('Action %s is not supported by this executor' % action_name)
            exit(1)

        handler_action_list = self.actions[action_name]
        queries_list = []
        for handler, action in handler_action_list:
            from pylib.hive.common import deploy_jars as dj
            if handler == dj:
                handler()
            else:
                queries_list.append(Stage(name=action.name, queries=self.evaluate_action(handler, action)))

        return queries_list

    def evaluate_action(self, handler, action):
        handler_args = []
        for param in action.params:
            if isinstance(param, Arg):
                handler_args += [getattr(self.args, param.attribute)]
            elif isinstance(param, Const):
                handler_args += [param.value]
            else:
                raise Exception('unrecognized handler parameter type: %s' % param.__class__.__name__)

        handler_kwargs = {}
        for named_param in action.kw_params:
            kw_param = action.kw_params[named_param]
            if isinstance(kw_param, Arg):
                handler_kwargs[named_param] = getattr(self.args, kw_param.attribute)
            elif isinstance(kw_param, Const):
                handler_kwargs[named_param] = kw_param.value
            else:
                raise Exception('unrecognized handler parameter type: %s' % kw_param.__class__.__name__)

        return handler(*handler_args, **handler_kwargs)
