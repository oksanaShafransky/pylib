#! /usr/bin/env python
import logging
import os
import sys

import datetime
from invoke import Program, Argument, Config
from invoke import ctask
from invoke.config import merge_dicts
from invoke.exceptions import Failure, ParseError, Exit

log = logging.getLogger('ptask_invoke')

# TODO: should check cross validation?
known_modes = ['snapshot', 'window', 'daily']
known_mode_types = ['monthly', 'last-28', 'daily']

class PtaskConfig(Config):
    @staticmethod
    def global_defaults():
        global_defaults = Config.global_defaults()
        global_defaults['sw_common'] = \
            {'date': None,
             'base_dir': '/similargroup/data',
             'force': True,
             'rerun': False}
        return global_defaults


def ptask(*args, **kwargs):
    return ctask(*args, **kwargs)


class PtaskInvoker(Program):
    def core_args(self):
        core_args = super(PtaskInvoker, self).core_args()
        extra_args = [
            Argument(names=('date', 'dt'), help="The task's logical day in ISO yyyy-MM-dd format", optional=True),
            Argument(names=('base_dir', 'bd'), help="The HDFS base directory for the task's output", optional=True),
            Argument(names=('mode', 'm'), help="Run mode (snapshot/window/daily)", optional=True),
            Argument(names=('mode_type', 'mt'), help="Run mode type (monthly/window/daily)", optional=True),
            Argument(names=('dont_force', 'df'), kind=bool,
                     help="Don't force flag - when used, the task will skip if expected output exists at start"),
            Argument(names=('rerun', 'rr'), kind=bool,
                     help="Rerun flag - when used, the task will use YARN reruns root queue")
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
        if self.args.mode.value:
            assert (self.args.mode.value in known_modes)
            sw_tasks['mode'] = self.args.mode.value
        if self.args.mode_type.value:
            assert (self.args.mode_type.value in known_mode_types)
            sw_tasks['mode_type'] = self.args.mode_type.value
        if self.args.dont_force.value:
            sw_tasks['force'] = False
        if self.args.rerun.value:
            sw_tasks['rerun'] = True
        sw_tasks['execution_user'] = os.environ['TASK_ID'].split('.')[0]
        sw_tasks['dag_id'] = os.environ['TASK_ID'].split('.')[1]
        sw_tasks['task_id'] = os.environ['TASK_ID'].split('.')[2]
        sw_tasks['execution_dt'] = os.environ['TASK_ID'].split('.')[3]

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
            # Restrict a run to one task at a time
            assert len(self.tasks) == 1
            print 'Starting ptask {0}'.format(os.environ['TASK_ID'])
            self.execute()
            print 'Finished successfuly ptask {0}'.format(os.environ['TASK_ID'].split('.')[2])
        except (Failure, Exit, ParseError) as e:
            log.warn("Received a possibly-skippable exception: {0!r}".format(e))
            # Print error message from parser if necessary.
            if isinstance(e, ParseError):
                sys.stderr.write("{0}\n".format(e))
            sys.exit(1)


def main():
    program = PtaskInvoker(config_class=PtaskConfig)
    program.run()


if __name__ == "__main__":
    main()
