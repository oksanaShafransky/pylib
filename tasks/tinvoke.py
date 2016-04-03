#! /usr/bin/env python
from invoke import Program, Argument, Config
from invoke.config import *
import datetime

class TaskConfig(Config):
    @staticmethod
    def global_defaults():
        global_defaults = Config.global_defaults()
        global_defaults['sw_common']=\
            {'date': None,
             'base_dir': '/similargroup/data',
             'force': True}
        return global_defaults


class TasksInvoker(Program):
    def core_args(self):
        core_args = super(TasksInvoker, self).core_args()
        extra_args = [
            Argument(names=('date','dt'), help="The task's logical day in ISO yyyy-MM-dd format", optional=True),
            Argument(names=('base_dir', 'bd'), help="The HDFS base directory for the task's output", optional=True),
            Argument(names=('dont_force', 'df'), kind=bool,
                     help="Don't force flag - when used, the task will skip if expected output exists at start")
        ]
        return core_args + extra_args

    @property
    def config(self):
        config = super(TasksInvoker, self).config

        sw_tasks = {}
        if self.args.date.value:
            sw_tasks['date'] = TasksInvoker.__parse_date(self.args.date.value)
        if self.args.base_dir.value:
            sw_tasks['base_dir'] = self.args.base_dir.value
        if self.args.dont_force.value:
            sw_tasks['force'] = False
        merge_dicts(config['sw_common'], sw_tasks)
        return config

    @staticmethod
    def __parse_date(date_str):
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()


def main():
    program = TasksInvoker(config_class=TaskConfig)
    program.run()


if __name__ == "__main__":
    main()
