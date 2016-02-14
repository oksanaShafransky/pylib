import shutil
import tempfile
import traceback

__author__ = 'Felix'

from inspect import isfunction

from common import Stage, logger
import hive_runner

from executer import Executer, Arg, Pool, CONCURRENCY


class HiveExecuter(Executer):
    def get_common_params(self):

        date_param = Arg('-d', '--date', 'date', Arg.date_arg, 'Date to use in %Y-%m-%d or %Y-%m format', required=True)
        mode_param = Arg('-m', '--mode', 'mode', ('daily', 'window', 'snapshot'), 'Job mode', required=True)
        mode_type_param = Arg('-mt', '--mode-type', 'mode_type', (
            'weekly', 'monthly', 'quarterly', 'annually', 'last-1', 'last-7', 'last-28', 'last-30', 'last-90'),
                              'Mode Type', required=False, default=None)
        num_reducers_param = Arg('-n', '--num-of-reducers', 'num_of_reducers', int, 'Number of reducers to use',
                                 required=False, default=32)
        sync_param = Arg('-s', '--sync', 'sync', bool, 'Run in sync mode (wait for completion', required=False,
                         default=False)
        dry_run_param = Arg('-dr', '--dry-run', 'dry_run', bool, 'If set, only output statements without running',
                            required=False, default=False)
        output_table_param = Arg('-o', '--output_table_path', 'output_table_path', str,
                                 'Output path root (not including the partition path', required=True)
        check_out_param = Arg('-co', '--check-output', 'check_output', bool, 'Return if output already exists',
                              required=False, default=False)
        merge_out_param = Arg('-dmo', '--dont-merge-output', 'no_merge_output', bool, 'Whether To Merge Output Files',
                              required=False, default=False)
        pool_param = Arg('-cp', '--calc-pool', 'calc_pool', str, 'Calculation pool to user', required=False,
                         default='calculation')
        compression_param = Arg('-cm', '--compression', 'compression', ('gz', 'bz2', 'none'), 'Compression type to use',
                                required=False, default='bz2')
        slow_start_param = Arg('-sscmr', '--slow-start-rate', 'slow_start_ratio', str,
                               'mapreduce.job.reduce.slowstart.completedmaps',
                               required=False, default=None)

        return [date_param, mode_param, mode_type_param, num_reducers_param, sync_param, dry_run_param,
                output_table_param, check_out_param, merge_out_param, pool_param, compression_param, slow_start_param]

    def get_arg_dependencies(self):

        return {
            ('mode', 'snapshot'): ('mode_type', 'monthly'),
            ('mode', 'window'): ('mode_type', 'last-28'),
            ('mode', 'daily'): ('mode_type', 'last-1')
        }

    def execute(self):
        steps = super(HiveExecuter, self).execute()

        self.results = {}

        if isinstance(steps, (list, tuple)):
            for step in steps:
                self.run_step(step, self.args)
        elif isinstance(steps, basestring):
            self.run_step(steps, self.args)  # Single item
        else:
            raise ValueError

        self.report_results()

        if 'failure' in self.results.values():
            return (1)

        return (0)

    def run_query_helper(self, arg_tuple):
        self.run_query(*arg_tuple)

    def run_step(self, stage, args):
        try:
            if isfunction(stage):
                if args.dry_run:
                    logger.info('DryRun, Was meant to execute %s' % stage.__name__)
                else:
                    stage()
            elif isinstance(stage, basestring):
                self.run_query(query_name=args.action,
                               query_str=stage,
                               args=args
                               )

            elif isinstance(stage, Stage):
                p = Pool(CONCURRENCY)
                stage_args = [(name, query_str, args) for name, query_str in stage.queries.items()]
                p.map(self.run_query_helper, stage_args)
        except:
            self.results[str(stage)] = 'failure'
            logger.error('Error! Stage failed.')
            traceback.print_exc()

    def run_query(self, query_name, query_str, args):

        job_params = [args.date.strftime('%Y-%m-%d'), args.mode]
        if 'key' in vars(args):
            job_params.append(args.key)
        if 'type' in vars(args):
            job_params.append(args.type)

        if args.dry_run:
            logger.info('DryRun:\n%s' % query_str)
            logger.info('Query is:\n%s' % query_str)
            return
        else:
            logger.info('Action Name:%s' % query_name)

        log_dir = tempfile.gettempdir() + '/logs/hive_exec/' + tempfile._get_candidate_names().next()
        logger.info('Hive log is at: %s' % log_dir)
        job_name = 'Hive. %s' % (' - '.join([query_name] + job_params))

        try:
            hive_runner.run_hive_job(hql=query_str, job_name=job_name, num_of_reducers=args.num_of_reducers,
                                     sync=args.sync, consolidate_output=not args.no_merge_output,
                                     log_dir=log_dir, slow_start_ratio=args.slow_start_ratio,
                                     calc_pool=args.calc_pool, compression=args.compression)
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
