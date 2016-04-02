#! /usr/bin/env python
from invoke import Program, Argument, Config


class TaskConfig(Config):
    @staticmethod
    def global_defaults():
        global_defaults = Config.global_defaults()
        global_defaults['run']['date'] = None
        global_defaults['run']['base_dir'] = '/similargroup/data'
        global_defaults['run']['force'] = False
        return global_defaults


class TasksInvoker(Program):
    def task_args(self):
        core_args = super(TasksInvoker, self).task_args()
        extra_args = [
            Argument(names=('date','dt'), help="The task's logical day in ISO yyyy-MM-dd format", optional=True),
            Argument(names=('base_dir', 'bd'), help="The HDFS base directory for the task's output", optional=True),
            Argument(names=('force', 'fr'), kind=bool, default=False,
                     help="Force flag - when false, the task will skip if expected output exists at start")
        ]
        return core_args + extra_args

    @property
    def config(self):
        run = {}
        if self.args['warn-only'].value:
            run['warn'] = True
        if self.args.pty.value:
            run['pty'] = True
        if self.args.hide.value:
            run['hide'] = self.args.hide.value
        if self.args.echo.value:
            run['echo'] = True
        if self.args.date.value:
            run['date'] = self.args.date.value
        if self.args.base_dir.value:
            run['base_dir'] = self.args.base_dir.value
        if self.args.force.value:
            run['force'] = self.args.force.value
        tasks = {}
        if self.args['no-dedupe'].value:
            tasks['dedupe'] = False
        overrides = {'run': run, 'tasks': tasks}
        # Stand up config object
        c = self.config_class(
                overrides=overrides,
                project_home=self.collection.loaded_from,
                runtime_path=self.args.config.value,
                env_prefix=self.env_prefix,
        )

        return c


def main():
    program = TasksInvoker(config_class=TaskConfig)
    program.run()


if __name__ == "__main__":
    main()
