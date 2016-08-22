import logging
import os
import re

import invoke
from datetime import datetime
from redis import StrictRedis

from pylib.tasks.ptask_infra import TasksInfra, ContextualizedTasksInfra
from pylib.tasks.ptask_invoke import PtaskConfig


class TestTasksInfra:
    def test_paths(self):
        dt = datetime(2016, 10, 03)
        assert TasksInfra.full_partition_path(date=dt, mode='window',
                                              mode_type='last-28') == 'type=last-28/year=16/month=10/day=03'
        assert TasksInfra.full_partition_path(date=dt, mode='snapshot',
                                              mode_type='monthly') == 'type=monthly/year=16/month=10'
        assert TasksInfra.full_partition_path(date=dt, mode='daily', mode_type=None) == 'year=16/month=10/day=03'
        assert TasksInfra.year_month_day(date=dt) == 'year=16/month=10/day=03'
        assert TasksInfra.year_month(date=dt) == 'year=16/month=10'

    def test_command_params(self):
        expected = 'cmd.py param1 param2 -action lol -switch -cats fluffles -cats duffles -cats muffles -number 32'
        options = {'switch': True,
                   'cats': ['fluffles', 'duffles', 'muffles'],
                   'action': 'lol',
                   'number': 32}
        actual = TasksInfra.add_command_params('cmd.py',
                                               options,
                                               'param1', 'param2')
        assert expected == actual


class TestContextualizedTasksInfra:
    def _disable_invoke_debug(self):
        log = logging.getLogger('invoke')
        log.setLevel(logging.ERROR)

    def test_python_run(self, monkeypatch):
        self._disable_invoke_debug()
        actual_commands = []
        expected_regexp = 'source .*/common.sh && pyexecute .*/test.py  -number 32'

        def mockrun(self, command, **kwargs):
            actual_commands.append(command)
            return invoke.runners.Result(command, 'mock', 'mock', 0, True)

        monkeypatch.setattr(invoke.runners.Runner, 'run', mockrun)
        config = PtaskConfig()
        ctx = invoke.context.Context(config)
        c_infra = ContextualizedTasksInfra(ctx)
        options = {'number': 32}
        c_infra.run_python('test.py', options)
        # Only one command runs
        assert len(actual_commands) == 1

        # The correct python command line is executed
        actual_command = actual_commands[0]
        assert re.match(expected_regexp, actual_command)

    def test_python_dry_run(self, monkeypatch):
        self._disable_invoke_debug()
        actual_commands = []

        def mockrun(self, command, **kwargs):
            actual_commands.append(command)
            return invoke.runners.Result(command, 'mock', 'mock', 0, True)

        monkeypatch.setattr(invoke.runners.Runner, 'run', mockrun)
        config = PtaskConfig()
        config['sw_common']['dry_run'] = True
        ctx = invoke.context.Context(config)
        c_infra = ContextualizedTasksInfra(ctx)
        options = {'number': 32}
        c_infra.run_python('test.py', options)
        # No commands run
        assert len(actual_commands) == 0

    def test_run_spark(self, monkeypatch):
        self._disable_invoke_debug()
        actual_commands = []
        expected_regexp = 'cd .*/mobile;spark-submit --queue research_shared .* --name "TestRun" .*--jars .*/test.jar,.*/test2.jar --class com.similarweb.mobile.Test ./mobile.jar   -number 32'

        def mockrun(self, command, **kwargs):
            actual_commands.append(command)
            return invoke.runners.Result(command, 'mock', 'mock', 0, True)

        def mock_listdir(path):
            return ['/test/test.jar', '/test/test2.jar']

        monkeypatch.setattr(invoke.runners.Runner, 'run', mockrun)
        monkeypatch.setattr(os, 'listdir', mock_listdir)

        config = PtaskConfig()
        ctx = invoke.context.Context(config)
        c_infra = ContextualizedTasksInfra(ctx)
        options = {'number': 32}
        c_infra.run_spark('com.similarweb.mobile.Test', 'mobile', 'research_shared', 'TestRun', options)
        # Only a single command is ran
        assert len(actual_commands) == 1
        # The correct python command line is executed
        actual_command = actual_commands[0]
        assert re.match(expected_regexp, actual_command)

    def test_log_lineage(self, monkeypatch):
        self._disable_invoke_debug()
        actual_values = []
        expected_values = [('LINEAGE_16-08-22', ('airflow.testDAG.testId.2016-08-12::output:hdfs::/tmp/test',)),
                           ('LINEAGE_16-08-22', ('airflow.testDAG.testId.2016-08-12::output:hdfs::/tmp/test2',))]

        def mock_rpush(self, name, *values):
            actual_values.append((name, values))

        monkeypatch.setattr(StrictRedis, 'rpush', mock_rpush)

        config = PtaskConfig()
        config['sw_common']['has_task_id'] = True
        config['sw_common']['dag_id'] = 'testDAG'
        config['sw_common']['task_id'] = 'testId'
        config['sw_common']['execution_dt'] = '2016-08-12'
        config['sw_common']['execution_user'] = 'airflow'
        ctx = invoke.context.Context(config)
        c_infra = ContextualizedTasksInfra(ctx)
        c_infra.log_lineage_hdfs(['/tmp/test', '/tmp/test2'], 'output')
        assert len(actual_values) == 2
        assert set(actual_values) == set(expected_values)
