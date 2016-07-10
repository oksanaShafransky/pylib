import datetime
import os
import sys

from invoke import Program, Argument, Config
from invoke import ctask
from invoke.config import merge_dicts
from invoke.exceptions import Failure, ParseError, Exit

# TODO: should check cross validation?
known_modes = ['snapshot', 'window', 'daily']
known_mode_types = ['monthly', 'last-28', 'daily']


class PtaskConfig(Config):
    @staticmethod
    def global_defaults():
        global_defaults = Config.global_defaults()
        global_defaults['sw_common'] = {'date': None,
                                        'base_dir': '/similargroup/data',
                                        'force': True,
                                        'rerun': False,
                                        'dry_run': False,
                                        'checks_only': False}
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
                     help="Rerun flag - when used, the task will use YARN reruns root queue"),
            Argument(names=('env_type', 'et'), help="Environment type (dev/staging/production)", optional=True),
            Argument(names=('dry_run', 'dr'), kind=bool, default=False, optional=True,
                     help="Some operations would only log their underlying command"),
            Argument(names=('checks_only', 'co'), kind=bool, default=False, optional=True,
                     help="Checks would run, executions would only print commands "),
            Argument(names=('table_prefix', 'tp'), help="Table Prefix", optional=True, default=''),
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
        if self.args.env_type.value:
            sw_tasks['env_type'] = self.args.env_type.value
        if self.args.table_prefix.value:
            sw_tasks['table_prefix'] = self.args.table_prefix.value
        if self.args.checks_only.value:
            sw_tasks['checks_only'] = True
        if self.args.dry_run.value:
            sw_tasks['dry_run'] = True

        if 'TASK_ID' in os.environ:
            sw_tasks['has_task_id'] = True
            sw_tasks['execution_user'] = os.environ['TASK_ID'].split('.')[0]
            sw_tasks['dag_id'] = os.environ['TASK_ID'].split('.')[1]
            sw_tasks['task_id'] = os.environ['TASK_ID'].split('.')[2]
            sw_tasks['execution_dt'] = os.environ['TASK_ID'].split('.')[3]
        else:
            sw_tasks['has_task_id'] = False

        merge_dicts(config['sw_common'], sw_tasks)
        return config

    @staticmethod
    def __parse_date(date_str):
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

    def run(self, argv=None, **kwargs):
        try:
            # add pylib to path
            sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
            self._parse(argv)
            # Restrict a run to one task at a time
            assert len(self.tasks) == 1
            if 'TASK_ID' in os.environ:
                task_name = os.environ['TASK_ID']
            else:
                task_name = self.tasks[0].name
            print '\nInvoking ptask "%(task_name)s" from "%(collection_name)s.py" ("%(collection_path)s")' % {
                'task_name': task_name,
                'collection_name': self.collection.name,
                'collection_path': self.collection.loaded_from
            }
            self.execute()
            print '\nFinished ptask "{0}"'.format(task_name)
        except (Failure, Exit, ParseError) as e:
            print 'Received a possibly-skippable exception: {0!r}'.format(e)
            if isinstance(e, ParseError):
                sys.stderr.write("{0}\n".format(e))
            sys.exit(1)


def main():
    import logging
    logging.root.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    logging.root.addHandler(ch)
    ptask_invoker = PtaskInvoker(config_class=PtaskConfig)
    ptask_invoker.run()


if __name__ == '__main__':
    main()
