import shutil
import tempfile
import traceback
import os
import shutil
from datetime import datetime
from inspect import isfunction
from multiprocessing.pool import ThreadPool as Pool

from hive_runner import HiveProcessRunner, HiveParamBuilder
from common import logger
from pylib.tasks.executer import Executer, Arg, Stage, CONCURRENCY
import six

__author__ = 'Felix'


class HiveExecuter(Executer):
    CACHED_FILES_STAGING_DIR = '/tmp/hive/cached_files'

    def __init__(self):
        super(HiveExecuter, self).__init__()
        self.cache_dir = '%s/%s' % (HiveExecuter.CACHED_FILES_STAGING_DIR, datetime.now().strftime('%Y.%m.%d.%H.%M.%S'))
        self.cached_files = []
        os.makedirs(self.cache_dir)

    def __del__(self):
        shutil.rmtree(self.cache_dir)

    def get_common_params(self):
        common_args = [
            Arg('-d', '--date', 'date', Arg.date_arg, 'Date to use in %Y-%m-%d or %Y-%m format', required=True),
            Arg('-m', '--mode', 'mode', ('daily', 'window', 'snapshot'), 'Job mode', required=True),
            Arg('-mt', '--mode-type', 'mode_type',
                ('weekly', 'monthly', 'quarterly', 'annually', 'daily', 'last-1', 'last-7', 'last-28', 'last-30', 'last-90'),
                'Mode Type', required=False, default=None),
            Arg('-n', '--num-of-reducers', 'num_of_reducers', int, '', required=False, default=32),
            Arg('-dr', '--dry-run', 'dry_run', bool, 'print generated statement only', required=False, default=False),
            Arg('-o', '--output_table_path', 'output_table_path', str,
                'Output path root (not including the partition path)', required=False),
            Arg('-co', '--check-output', 'check_output', bool, 'Return if output already exists', required=False,
                default=False),
            Arg('-dmo', '--dont-merge-output', 'no_merge_output', bool, 'Whether To Merge Output Files', required=False,
                default=False),
            Arg('-cp', '--calc-pool', 'calc_pool', str, 'Calculation pool to use', required=False,
                default='calculation'),
            Arg('-cm', '--compression', 'compression', ('gz', 'bz2', 'none'), 'Compression type to use', required=False,
                default='bz2'),
            Arg('-sscmr', '--slow-start-rate', 'slow_start_ratio', str,
                'set mapreduce.job.reduce.slowstart.completedmaps', required=False, default=None),
            Arg('-hdb', '--hive-database', 'hive_db', str, 'hive db to use', required=False, default='analytics'),
            Arg('-tm', '--task-memory', 'task_mem', int, 'Task Memory (MB)', required=False, default=4096),
            Arg('-ibs', '--input-block-size', 'block_size', int, 'Input Block Size (MB)', required=False, default=None),
        ]
        return common_args

    def get_arg_dependencies(self):

        return {
            ('mode', 'snapshot'): ('mode_type', 'monthly'),
            ('mode', 'window'): ('mode_type', 'last-28'),
            ('mode', 'daily'): ('mode_type', 'last-1')
        }

    def setup(self):
        pass

    def cleanup(self):
        pass

    def execute(self):
        steps = super(HiveExecuter, self).execute()
        self.setup()

        self.results = {}

        if isinstance(steps, (list, tuple)):
            for step in steps:
                self.run_step(step, self.args)
        elif isinstance(steps, six.string_types):
            self.run_step(steps, self.args)  # Single item
        else:
            raise ValueError

        self.report_results()

        self.cleanup()

        if 'failure' in self.results.values():
            return 1

        return 0

    def run_query_helper(self, arg_tuple):
        self.run_query(*arg_tuple)

    def run_step(self, stage, args, register=True):
        try:
            if isinstance(stage, Stage):
                self.run_step(stage.queries, args, False)
            elif isinstance(stage, (list, tuple)):
                for sub_stage in stage:
                    self.run_step(sub_stage, args, False)
            elif isfunction(stage):
                if args.dry_run:
                    logger.info('DryRun, Was meant to execute %s' % stage.__name__)
                else:
                    stage()
            elif isinstance(stage, six.string_types):
                self.run_query(query_name=args.action,
                               query_str=stage,
                               args=args
                               )

            else:
                p = Pool(CONCURRENCY)
                stage_args = [(name, query_str, args) for name, query_str in stage.queries]
                p.map(self.run_query_helper, stage_args)
                p.close()
        except:
            if register:
                self.results[str(stage)] = 'failure'
            logger.error('Error! Stage failed.')
            traceback.print_exc()

    def run_query(self, query_name, query_str, args):

        # register cached files
        for cached_file in self.cached_files:
            query_str = 'ADD FILE %s/%s; \n%s' % (self.cache_dir, cached_file, query_str)

        job_params = [args.date.strftime('%Y-%m-%d'), args.mode]
        if 'key' in vars(args):
            job_params.append(args.key)
        if 'type' in vars(args):
            job_params.append(args.type)

        logger.info('Action Name:%s' % query_name)
        if args.dry_run:
            logger.info('DryRun Query :\n%s' % query_str)
            return

        log_dir = tempfile.gettempdir() + '/logs/hive_exec/' + tempfile._get_candidate_names().next()
        logger.info('Hive log is at: %s' % log_dir)
        job_name = 'Hive. %s' % (' - '.join([query_name] + job_params))

        try:
            hive_params = HiveParamBuilder() \
                .set_pool(args.calc_pool) \
                .with_memory(args.task_mem) \
                .with_input_block_size(args.block_size, condition=args.block_size is not None) \
                .set_compression(args.compression) \
                .start_reduce_early(args.slow_start_ratio, condition=args.slow_start_ratio is not None) \
                .consolidate_output(not args.no_merge_output)

            HiveProcessRunner().run_query(query_str, hive_params, job_name=job_name, partitions=args.num_of_reducers, log_dir=log_dir, is_dry_run=args.dry_run)
            self.results[query_name] = 'success'
        except:
            self.results[query_name] = 'failure'
            traceback.print_exc()
        finally:
            try:
                shutil.rmtree(log_dir)
            except:
                logger.error('failed removing log dir')

    def report_results(self):
        logger.info('reporting execution summary\n')
        for key in self.results:
            logger.info('%s: %s' % (key, self.results[key]))

    # caches an hdfs file to each spawned job
    def cache_file(self, path, name):
        cached_file_name = name if name is not None else path.split('/')[-1]
        logger.info('caching file %s as %s' % (path, cached_file_name))
        self.cached_files += [cached_file_name]
        from pylib.hadoop.hdfs_util import get_file
        get_file(path, '%s/%s' % (self.cache_dir, cached_file_name))
