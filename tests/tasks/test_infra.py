import logging
import os
import re

import invoke
import datetime

import pytest
from pylib.hive import table_utils
from redis import StrictRedis

from pylib.tasks.ptask_infra import TasksInfra, ContextualizedTasksInfra
from pylib.tasks.ptask_invoke import PtaskConfig


class TestTasksInfra(object):
    def test_paths(self):
        dt = datetime.datetime(2016, 10, 3)
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
                                               '',
                                               'param1', 'param2')
        assert expected == actual

    def test_jvm_options(self):
        expected = 'cmd.py -D cats.fluffnessEnabled=True -D cats.lives=9 -D yarn.string.chaser=duffles -D yarn.string.entangler=fluffles'
        options = {'yarn.string.entangler': 'fluffles',
                   'yarn.string.chaser': 'duffles',
                   'cats.fluffnessEnabled': True,
                   'cats.lives': 9}
        actual = TasksInfra.add_jvm_options('cmd.py', options)
        split = actual.split(' -D')
        actual = ' -D'.join([split[0]] + sorted(split[1:]))
        assert expected == actual

        expected = 'cmd.py'
        actual = TasksInfra.add_jvm_options('cmd.py', None)
        assert expected == actual

    def test_command_value_wrapping(self):
        wrapper = '$'
        expected = 'cmd.py param1 param2 -action %(wrap)slol%(wrap)s -switch -cats %(wrap)sfluffles%(wrap)s -cats %(wrap)sduffles%(wrap)s -cats %(wrap)smuffles%(wrap)s -number %(wrap)s32%(wrap)s' % \
                   {
                       'wrap': wrapper
                   }
        options = {'switch': True,
                   'cats': ['fluffles', 'duffles', 'muffles'],
                   'action': 'lol',
                   'number': 32}
        actual = TasksInfra.add_command_params('cmd.py',
                                               options,
                                               wrapper,
                                               'param1', 'param2')
        assert expected == actual


class TestContextualizedTasksInfra(object):
    def _disable_invoke_debug(self):
        log = logging.getLogger('invoke')
        log.setLevel(logging.ERROR)

    def test_python_run(self, monkeypatch):
        self._disable_invoke_debug()
        actual_commands = []
        expected_regexp = 'source .*/common.sh && pyexecute .*/test.py  -number %(wrap)s32%(wrap)s' % \
                          {
                              'wrap': TasksInfra.EXEC_WRAPPERS['python']
                          }

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
        expected_regexp = '''cd .*/mobile;spark-submit --queue research_shared .* --name "TestRun" .*--conf key1\=val1 --key2 val2  --jars .*/test.jar,.*/test2.jar --files "" --class com.similarweb.mobile.Test ./mobile.jar   -number %(wrap)s32%(wrap)s''' \
            % \
                {
                    'wrap': TasksInfra.EXEC_WRAPPERS['bash']
                }

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
        configs = {'key1': 'val1'}
        named_args = {'key2': 'val2'}
        c_infra.run_spark('com.similarweb.mobile.Test', 'mobile', 'research_shared', 'TestRun', options, spark_configs=configs, named_spark_args=named_args)
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

        class NewDate(datetime.date):
            @classmethod
            def today(cls):
                return cls(2016, 8, 22)

        monkeypatch.setattr(StrictRedis, 'rpush', mock_rpush)
        monkeypatch.setattr(datetime, 'date', NewDate)


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

    @staticmethod
    @pytest.fixture
    def get_jars_list_fixture(monkeypatch):
        config = PtaskConfig()
        ctx = invoke.context.Context(config)
        c_infra = ContextualizedTasksInfra(ctx)

        def mock_listdir(path):
            return ['httpcore-4.4.5.jar', 'annotations-15.0.jar', 'httpcore-nio-4.4.5.jar']
        monkeypatch.setattr(os, 'listdir', mock_listdir)

        return c_infra

    def test_get_jars_list_unversioned(self, get_jars_list_fixture):
        self._disable_invoke_debug()

        jars = get_jars_list_fixture.get_jars_list(module_dir='analytics', jars_from_lib=['httpcore.jar', 'annotations.jar'])
        assert jars == 'analytics/lib/httpcore-4.4.5.jar,analytics/lib/httpcore-nio-4.4.5.jar,analytics/lib/annotations-15.0.jar'

    def test_get_jars_list_versioned(self, get_jars_list_fixture):
        self._disable_invoke_debug()

        jars = get_jars_list_fixture.get_jars_list(module_dir='analytics', jars_from_lib=['annotations-15.0.jar'])
        assert jars == 'analytics/lib/annotations-15.0.jar'

    def test_get_jars_list_wrong_version(self, get_jars_list_fixture):
        self._disable_invoke_debug()

        with pytest.raises(AssertionError):
            get_jars_list_fixture.get_jars_list(module_dir='analytics', jars_from_lib=['httpcore.jar', 'annotations-16.0.jar'])

    def test_get_jars_list_extraneous_jar(self, get_jars_list_fixture):
        self._disable_invoke_debug()

        with pytest.raises(AssertionError):
            get_jars_list_fixture.get_jars_list(module_dir='analytics', jars_from_lib=['alternativefact.jar',
                                                                                 'annotations.jar'])


    #TODO Finalize unit test for assert_hbase_snapshot_exists
    # def test_assert_hbase_snapshot_exists(self):
    #     config = PtaskConfig()
    #     ctx = invoke.context.Context(config)
    #     c_infra = ContextualizedTasksInfra(ctx)
    #
    #     c_infra.assert_hbase_snapshot_exists('top_lists_17_02_barak_test', '/hbase', 'hbase-production-hdfs-nn.service.consul')
    #



    class TestDaysInRange(object):

        def test_monthly_mode_type(self):
            days = list(TasksInfra.days_in_range(datetime.date(2017, 1, 15), 'monthly'))
            assert len(days) == 31
            for day in days:
                assert day.month == 1
            assert days[0].day == 1
            assert days[30].day == 31

        def test_last_28_mode_type(self):
            days = list(TasksInfra.days_in_range(datetime.date(2017, 1, 15), 'last-28'))
            assert len(days) == 28
            assert days[0] == datetime.date(2016, 12, 19)
            assert days[27] == datetime.date(2017, 1, 15)

        def test_unknown_mode_type(self):
            with pytest.raises(ValueError):
                list(TasksInfra.days_in_range(datetime.date(2017, 1, 15), 'unknown'))
