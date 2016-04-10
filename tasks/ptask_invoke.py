#! /usr/bin/env python
from invoke import Program, Argument, Config
from invoke.config import *
import datetime
from invoke.exceptions import Failure, ParseError, Exit
from invoke.util import debug
from invoke import ctask
import os
import sys

class PtaskConfig(Config):
    @staticmethod
    def global_defaults():
        global_defaults = Config.global_defaults()
        global_defaults['sw_common']=\
            {'date': None,
             'base_dir': '/similargroup/data',
             'force': True}
        return global_defaults


def ptask(*args, **kwargs):
    return ctask(*args, **kwargs)


class PtaskInvoker(Program):
    def core_args(self):
        core_args = super(PtaskInvoker, self).core_args()
        extra_args = [
            Argument(names=('date','dt'), help="The task's logical day in ISO yyyy-MM-dd format", optional=True),
            Argument(names=('base_dir', 'bd'), help="The HDFS base directory for the task's output", optional=True),
            Argument(names=('dont_force', 'df'), kind=bool,
                     help="Don't force flag - when used, the task will skip if expected output exists at start")
        ]
        return core_args + extra_args

    @property
    def config(self):
        config = super(PtaskInvoker, self).config

        sw_tasks = {}
        if self.args.date.value:
            sw_tasks['date'] = PtaskInvoker.__parse_date(self.args.date.value)
        if self.args.base_dir.value:
            sw_tasks['base_dir'] = self.args.base_dir.value
        if self.args.dont_force.value:
            sw_tasks['force'] = False
        merge_dicts(config['sw_common'], sw_tasks)
        return config

    @staticmethod
    def __parse_date(date_str):
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

    def run(self, argv=None):
        try:
            # add pylib to path
            sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
            self._parse(argv)
            self.execute()
        except (Failure, Exit, ParseError) as e:
            debug("Received a possibly-skippable exception: {0!r}".format(e))
            # Print error message from parser if necessary.
            if isinstance(e, ParseError):
                sys.stderr.write("{0}\n".format(e))
            sys.exit(1)

def main():
    program = PtaskInvoker(config_class=PtaskConfig)
    program.run()


if __name__ == "__main__":
    main()
