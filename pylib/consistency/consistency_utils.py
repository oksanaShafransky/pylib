from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import pylib.hive.common as common


class ConsistencyTestInfra(object):
    def __init__(self, ti):
        self.ti = ti
        self.default_spark_args = {
            'num-executors': 100,
            'executor-memory': '4G',
            'driver-memory': '10G',
            'master': 'yarn-cluster'
        }

        self.default_spark_config = {
            'spark.yarn.executor.memoryOverhead': '1024'
        }

    @staticmethod
    def _gen_model_paths(base_dir, test_name, model_date, has_day_partition, countries_list):
        date_partition = common.get_date_partition_path(
            year=model_date.year % 100,
            month=model_date.month,
            day=model_date.day if has_day_partition else None
        )

        def gen_path_for_country(country):
            return '%(base_dir)s/consistency/model/%(name)s/%(date_part)s/country=%(country)s' % \
                   {
                       'base_dir': base_dir,
                       'type': 'day' if has_day_partition else 'month',
                       'name': test_name,
                       'country': country,
                       'date_part': date_partition
                   }

        return [gen_path_for_country(country) for country in countries_list]

    @staticmethod
    def _gen_result_paths(base_dir, test_name, test_date, has_day_partition, countries_list):
        date_partition = common.get_date_partition_path(
            year=test_date.year % 100,
            month=test_date.month,
            day=test_date.day if has_day_partition else None
        )

        def gen_path_for_country(country):
            return '%(base_dir)s/consistency/output/%(name)s/type=bycountry/%(date)s/country=%(country)s' % \
                   {
                       'base_dir': base_dir,
                       'name': test_name,
                       'country': country,
                       'date': date_partition
                   }

        return [gen_path_for_country(country) for country in countries_list]

    @staticmethod
    def _gen_grouped_result_path(base_dir, test_name, test_date, has_day_partition):
        date_partition = common.get_date_partition_path(
            year=test_date.year % 100,  # two-digits year
            month=test_date.month,
            day=test_date.day if has_day_partition else None
        )
        return '%(base_dir)s/consistency/output/%(name)s/type=grouped/%(date)s' % \
               {
                   'base_dir': base_dir,
                   'name': test_name,
                   'date': date_partition
               }

    @staticmethod
    def _gen_input_dates(base_date, date_type='day', sample_count=5):
        if date_type == 'month':
            # take 5 month dates fro base_date backwards (day 1 is not actually used later)
            return [date(
                (base_date - relativedelta(months=x)).year,
                (base_date - relativedelta(months=x)).month,
                1
            ) for x in range(0, sample_count)]
        elif date_type == 'day_of_week':
            # Take 5 day dates with week intervals
            return [base_date - relativedelta(weeks=x) for x in range(0, sample_count)]
        elif date_type == 'day':
            # Take 3 day dates with day intervals
            return [base_date - relativedelta(days=x) for x in range(0, sample_count)]
        else:
            return []

    @staticmethod
    def _gen_input_paths(base_dir, path, base_date, date_type):
        input_dates = ConsistencyTestInfra._gen_input_dates(base_date, date_type)

        def get_date_part(d):
            return common.get_date_partition_path(
                d.year % 100,  # two-digits year
                d.month,
                d.day if date_type is not 'month' else None
            )
        return ['%(base_dir)s%(path)s/%(date_part)s' %
                {
                    'base_dir': base_dir,
                    'path': path,
                    'date_part': get_date_part(d)

                } for d in input_dates]

    @staticmethod
    def _get_model_training_command_params(
            test_name,
            data_date,
            source_db,
            source_table,
            data_column,
            has_day_partition,
            platform,
            date_type,
            countries,
            output_path,
            reverse_dates,
            sites_visit_filter,
            apps_rank_filter,
            entity_column,
            num_features,
            sig_noise_coeff,
            std_cp,
            avg_cp,
            std_ss,
            avg_ss,
            tv_gauss,
            tv_symm):

        # There's a '-' sign in the key because run_py_spark only ads one '-' and we need two
        params = {
            '-test_name': test_name,
            '-date': data_date,
            '-countries': countries,
            '-source_db': source_db,
            '-source_table': source_table,
            '-data_column': data_column,
            '-platform': platform,
            '-date_type': date_type,
            '-output_path': output_path,
            '-reverse_dates': reverse_dates,
            '-apps_rank_filter': apps_rank_filter,
            '-sites_visit_filter': sites_visit_filter,
            '-has_day_partition': has_day_partition,
            '-entity_column': entity_column,
            '-num_features': num_features,
            '-sig_noise_coeff': sig_noise_coeff,
            '-std_cp': std_cp,
            '-avg_cp': avg_cp,
            '-std_ss': std_ss,
            '-avg_ss': avg_ss,
            '-tv_gauss': tv_gauss,
            '-tv_symm': tv_symm
        }

        return params

    def run_consistency_py_spark(
            self,
            main_py_file,
            command_params,
            output_dirs,
            named_spark_args=None,
            spark_configs=None,
            queue='calculation'
    ):

        # Take default spark args and override with given parameters
        actual_named_spark_args = self.default_spark_args.copy()
        if named_spark_args is not None:
            actual_named_spark_args.update(named_spark_args)

        # Take default spark config and override with given parameters
        actual_spark_configs = self.default_spark_config.copy()
        if spark_configs is not None:
            actual_spark_configs.update(spark_configs)

        # set default values where no value present
        if 'spark.yarn.executor.memoryOverhead' not in actual_spark_configs:
            actual_spark_configs['spark.yarn.executor.memoryOverhead'] = '1024'

        self.ti.run_py_spark(
            app_name='Consistency Test For %s' % command_params['-test_name'],
            main_py_file='drivers/' + main_py_file,
            command_params=command_params,
            module='consistency',
            queue=queue,
            files=['/etc/hive/conf/hive-site.xml'],
            py_files=[
                self.ti.execution_dir + '/consistency/consistency-0.0.0.dev0-py2.7.egg',
                self.ti.execution_dir + '/sw-spark-common/sw_spark-0.0.0.dev0-py2.7.egg'
            ],
            named_spark_args=actual_named_spark_args,
            spark_configs=actual_spark_configs,
            use_bigdata_defaults=True,
            managed_output_dirs=output_dirs
        )

    @staticmethod
    def _get_consistency_test_command_params(
            test_name,
            data_date,
            source_db,
            source_table,
            data_column,
            output_path,
            model_base_dir,
            has_day_partition,
            countries_list,
            platform,
            date_type,
            reverse_dates,
            entity_column,
            cp_threshold,
            model_date,
            email_to):

        print('Running test %s with model %s' % (test_name, model_date))

        # There's a '-' sign in the key because run_py_spark only ads one '-' and we need two
        params = {
            '-test_name': test_name,
            '-date': data_date,
            '-countries': ','.join(map(str, countries_list)),
            '-source_db': source_db,
            '-source_table': source_table,
            '-data_column': data_column,
            '-output_path': output_path,
            '-model_base_dir': model_base_dir,
            '-platform': platform,
            '-date_type': date_type,
            '-reverse_dates': reverse_dates,
            '-has_day_partition': has_day_partition,
            '-entity_column': entity_column,
            '-cp_threshold': cp_threshold,
            '-email_to': email_to,
            '-model_date': model_date
        }
        return params

    def train_model(
            self,
            test_name,
            source_db,
            source_table,
            data_column,
            input_path,
            platform,
            has_day_partition,
            date_type,
            countries,
            reverse_dates=None,
            sites_visit_filter=100000,
            apps_rank_filter='0-2000',
            entity_column=None,
            num_features=2,
            sig_noise_coeff=0.01,
            std_cp=0.036,
            avg_cp=0.0586,
            std_ss=0.05,
            avg_ss=0.22,
            tv_gauss=2.6,
            tv_symm=1.0,
            named_spark_args=None,
            spark_configs=None,
            spark_queue='calculation'
    ):
        """
        :param test_name: unique test name - used for hdfs path generation and spark job name
        :param source_db: hive db for data endpoint
        :param source_table: hive table for data endpoint
        :param data_column: column in hive table for data endpoint
        :param input_path: path (without base_dir prefix) to source data. used for input assertion.
            source_db and source_table should be on top of this path
        :param platform: specifies data platform, used for querying - web/apps
        :param has_day_partition: bool - does source data has daily partition
        :param date_type: type date for data points - used for querying:
            daily - consecutive daily endpoints
            day_of_week - daily data in weekly intervals
            monthly - monthly data
        :param countries: comma separated  list of country codes
        :param reverse_dates: should data points be reversed, i.e test date against future data.
            default is False (test against past data)
        :param sites_visit_filter: lower visits threshold websites filtering
        :param apps_rank_filter: filter for apps rank. string format is a-b
        :param entity_column: entity column in data is usually 'site' or 'app', if it is different, use this
            argument to override
        :param num_features:
        :param sig_noise_coeff: signal to noise coefficient
        :param std_cp: std of cp for country alarm
        :param avg_cp: avg of cp for country alarm
        :param std_ss: std of significant slope for country alarm
        :param avg_ss: avg of significant slope for country alarm
        :param tv_gauss: clip value for gauss
        :param tv_symm: clip value for symmetry
        :param named_spark_args: named spark args dictionary to override defaults
        :param spark_configs: spark configs dictionary to override defaults
        :param spark_queue: override queue (default is 'calculation')
        """
        input_paths = ConsistencyTestInfra._gen_input_paths(
            base_dir=self.ti.base_dir,
            path=input_path,
            base_date=self.ti.date,
            date_type=date_type
        )
        self.ti.assert_input_validity(input_paths, validate_marker=True)

        output_path = self.ti.calc_dir if self.ti.calc_dir else self.ti.base_dir
        command_params = ConsistencyTestInfra._get_model_training_command_params(
            test_name=test_name,
            data_date=self.ti.date,
            source_db=source_db,
            source_table=source_table,
            data_column=data_column,
            has_day_partition=has_day_partition,
            platform=platform,
            date_type=date_type,
            countries=countries,
            output_path=output_path,
            reverse_dates=reverse_dates,
            sites_visit_filter=sites_visit_filter,
            apps_rank_filter=apps_rank_filter,
            entity_column=entity_column,
            num_features=num_features,
            sig_noise_coeff=sig_noise_coeff,
            std_cp=std_cp,
            avg_cp=avg_cp,
            std_ss=std_ss,
            avg_ss=avg_ss,
            tv_gauss=tv_gauss,
            tv_symm=tv_symm)

        countries_list = countries.split(',')
        # output checks
        model_paths = ConsistencyTestInfra._gen_model_paths(
            base_dir=output_path,
            test_name=test_name,
            has_day_partition=has_day_partition,
            model_date=self.ti.date,
            countries_list=countries_list
        )

        self.run_consistency_py_spark(
            main_py_file='consistency_model_driver.py',
            command_params=command_params,
            output_dirs=model_paths,
            named_spark_args=named_spark_args,
            spark_configs=spark_configs,
            queue=spark_queue
        )

        self.ti.assert_output_validity(model_paths, min_size_bytes=10, validate_marker=True)

    def test(
            self,
            test_name,
            source_db,
            source_table,
            data_column,
            has_day_partition,
            platform,
            date_type,
            countries,
            reverse_dates=None,
            entity_column=None,
            cp_threshold=0.90,
            model_base_dir=None,
            model_date=None,
            email_to=None,
            named_spark_args=None,
            spark_configs=None,
            spark_queue='calculation'
    ):
        """
        :param test_name: unique test name - used for hdfs path generation and spark job name
        :param source_db: hive db for data endpoint
        :param source_table: hive table for data endpoint
        :param data_column: column in hive table for data endpoint
        :param has_day_partition: bool - does source data has daily partition
        :param platform: specifies data platform, used for querying - web/apps
        :param date_type: type date for data points - used for querying:
            daily - consecutive daily endpoints
            day_of_week - daily data in weekly intervals
            monthly - monthly data
        :param countries: comma separated  list of country codes
        :param reverse_dates: should data points be reversed, i.e test date against future data.
            default is False (test against past data)
        :param entity_column: entity column in data is usually 'site' or 'app', if it is different, use this
            argument to override
        :param cp_threshold:
        :param model_base_dir: custom base dir from which model is taken
            (use calc_dir when training model to specify custom output dir)
        :param model_date: date for specific model to use. format yyyy-mm-dd. If not specified active model is used
        :param email_to: email to send test report to
        :param named_spark_args: named spark args dictionary to override defaults
        :param spark_configs: spark configs dictionary to override defaults
        :param spark_queue: override queue (default is 'calculation')
        """
        countries_list = map(int, countries.split(','))

        # input checks for model only if specific model is requested
        model_date_parsed = None
        if model_date:
            model_base_dir = model_base_dir if model_base_dir is not None else self.ti.base_dir
            model_date_parsed = datetime.strptime(model_date, '%Y-%m-%d').date()
            model_paths = ConsistencyTestInfra._gen_model_paths(
                base_dir=model_base_dir,
                test_name=test_name,
                model_date=model_date_parsed,
                has_day_partition=has_day_partition,
                countries_list=countries_list
            )
            self.ti.assert_input_validity(model_paths, min_size_bytes=10, validate_marker=True)

        output_path = self.ti.calc_dir if self.ti.calc_dir else self.ti.base_dir
        command_params = ConsistencyTestInfra._get_consistency_test_command_params(
            test_name=test_name,
            data_date=self.ti.date,
            source_db=source_db,
            source_table=source_table,
            data_column=data_column,
            has_day_partition=has_day_partition,
            output_path=output_path,
            model_base_dir=model_base_dir,
            platform=platform,
            date_type=date_type,
            countries_list=countries_list,
            reverse_dates=reverse_dates,
            entity_column=entity_column,
            cp_threshold=cp_threshold,
            model_date=model_date_parsed if model_date else None,
            email_to=email_to
        )

        result_paths = ConsistencyTestInfra._gen_result_paths(
            base_dir=self.ti.base_dir,
            test_name=test_name,
            test_date=self.ti.date,
            countries_list=countries_list,
            has_day_partition=has_day_partition
        )

        self.run_consistency_py_spark(
            main_py_file='consistency_test_driver.py',
            command_params=command_params,
            output_dirs=result_paths,
            named_spark_args=named_spark_args,
            spark_configs=spark_configs,
            queue=spark_queue
        )

        # output checks
        self.ti.assert_output_validity(result_paths, min_size_bytes=100, validate_marker=True)
        grouped_result_path = ConsistencyTestInfra._gen_grouped_result_path(
            base_dir=self.ti.base_dir,
            test_name=test_name,
            test_date=self.ti.date,
            has_day_partition=has_day_partition
        )
        self.ti.assert_output_validity(grouped_result_path, min_size_bytes=100, validate_marker=True)
