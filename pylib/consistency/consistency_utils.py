from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import pylib.hive.common as common
from pylib.consistency.consistency_types import ConsistencyOutputType
from pylib.consistency.paths import ConsistencyPaths
from pylib.tasks.data import DataArtifact
from pylib.tasks.input_data_artifact import InputDataArtifact
from pylib.tasks.output_data_artifact import OutputDataArtifact


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
    def _get_model_training_command_params(
            test_name,
            input_paths,
            data_column,
            platform,
            data_date,
            countries,
            tsv_schema_table,
            data_column_threshold,
            reverse_dates,
            base_output_path,
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
            '-input_paths': ','.join(input_paths),
            '-data_column': data_column,
            '-platform': platform,
            '-date': data_date,
            '-countries': countries,
            '-tsv_schema_table': tsv_schema_table,
            '-data_column_threshold': data_column_threshold,
            '-reverse_dates': reverse_dates,
            '-base_output_path': base_output_path,
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
            packages=['com.databricks:spark-csv_2.10:1.5.0'],
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
            input_paths,
            data_column,
            platform,
            countries_list,
            reverse_dates,
            tsv_schema_table,
            data_column_threshold,
            base_model_path,
            model_for_test_base_path,
            country_result_base_path,
            total_results_path,
            benchmark_path,
            email_to,
            cp_threshold,
            std_cp,
            avg_cp,
            std_ss,
            avg_ss,
            benchmark_mode
    ):

        # There's a '-' sign in the key because run_py_spark only ads one '-' and we need two
        params = {
            '-test_name': test_name,
            '-date': data_date,
            '-countries': ','.join(map(str, countries_list)),
            '-email_to': email_to,

            '-base_model_path': base_model_path,
            '-model_for_test_base_path': model_for_test_base_path,
            '-country_result_base_path': country_result_base_path,
            '-total_results_path': total_results_path,
            '-benchmark_path': benchmark_path,

            '-cp_threshold': cp_threshold,
            '-std_cp': std_cp,
            '-avg_cp': avg_cp,
            '-std_ss': std_ss,
            '-avg_ss': avg_ss,

            '-input_paths': ','.join(input_paths),
            '-data_column': data_column,
            '-platform': platform,
            '-reverse_dates': reverse_dates,
            '-tsv_schema_table': tsv_schema_table,
            '-data_column_threshold': data_column_threshold,

            '-benchmark_mode': benchmark_mode
        }
        return params

    def train_model(
            self,
            test_name,
            input_path,
            data_column,
            platform,
            date_type,
            countries,
            tsv_schema_table=None,
            data_column_threshold=None,
            reverse_dates=None,
            cp_threshold=0.9,
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
        :param input_path: path (without base_dir prefix) to source data
        :param data_column: column in hive table for data endpoint
        :param platform: specifies data platform, used for querying - web/apps
        :param date_type: type date for data points - used for querying:
            day - consecutive daily endpoints
            day_of_week - daily data in weekly intervals
            month - monthly data
        :param countries: comma separated  list of country codes
        :param tsv_schema_table: full name of hive table define above the input path (in case the data is tsv and not parquet)
        :param data_column_threshold: minimum threshold of values to use from data column (min visits or unique for example)
        :param reverse_dates: should data points be reversed, i.e test date against future data.
            default is False (test against past data)
        :param num_features:
        :param cp_threshold:
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
        input_paths = ConsistencyPaths.gen_input_paths(
            base_dir=self.ti.base_dir,
            path=input_path,
            base_date=self.ti.date,
            date_type=date_type
        )
        self.ti.set_s3_keys()

        def resolve_path(p):
            da = InputDataArtifact(self.ti, p, required_marker=False)
            return da.resolved_path

        base_model_path = ConsistencyPaths.gen_base_model_path(
            base_dir=self.ti.calc_dir,
            name=test_name,
            model_date=self.ti.date,
            date_type=date_type
        )

        command_params = ConsistencyTestInfra._get_model_training_command_params(
            test_name=test_name,
            input_paths=map(resolve_path, input_paths),
            data_column=data_column,
            platform=platform,
            data_date=self.ti.date,
            countries=countries,
            tsv_schema_table=tsv_schema_table,
            data_column_threshold=data_column_threshold,
            reverse_dates=reverse_dates,
            base_output_path=base_model_path,
            num_features=num_features,
            sig_noise_coeff=sig_noise_coeff,
            std_cp=std_cp,
            avg_cp=avg_cp,
            std_ss=std_ss,
            avg_ss=avg_ss,
            tv_gauss=tv_gauss,
            tv_symm=tv_symm
        )

        countries_list = countries.split(',')
        # output checks and managed output dirs
        model_paths = ConsistencyPaths.gen_all_model_paths(
            base_dir=self.ti.calc_dir,
            name=test_name,
            model_date=self.ti.date,
            countries=countries_list,
            date_type=date_type

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

        # run benchmark test
        self.test(
            test_name=test_name,
            input_path=input_path,
            data_column=data_column,
            platform=platform,
            date_type=date_type,
            countries=countries,
            model_date=self.ti.date,
            model_base_dir=self.ti.calc_dir,
            tsv_schema_table=tsv_schema_table,
            data_column_threshold=data_column_threshold,
            reverse_dates=reverse_dates,
            cp_threshold=cp_threshold,
            named_spark_args=named_spark_args,
            spark_configs=spark_configs,
            spark_queue=spark_queue,
            benchmark_mode=True)

    def test(
            self,
            test_name,
            input_path,
            data_column,
            platform,
            date_type,
            countries,
            model_date,
            tsv_schema_table=None,
            data_column_threshold=None,
            reverse_dates=None,
            cp_threshold=0.90,
            model_base_dir=None,
            email_to=None,
            std_cp=0.036,
            avg_cp=0.0586,
            std_ss=0.05,
            avg_ss=0.22,
            named_spark_args=None,
            spark_configs=None,
            spark_queue='calculation',
            benchmark_mode=False
    ):
        """
        :param test_name: unique test name - used for hdfs path generation and spark job name
        :param input_path: path (without base_dir prefix) to source data
        :param data_column: column in hive table for data endpoint
        :param platform: specifies data platform, used for querying - web/apps
        :param date_type: type date for data points - used for querying:
            day - consecutive daily endpoints
            day_of_week - daily data in weekly intervals
            month - monthly data
        :param countries: comma separated  list of country codes
        :param model_date: date for specific model to use. datetime.date object.
        :param tsv_schema_table: full name of hive table define above the input path (in case the data is tsv and not parquet)
        :param data_column_threshold: minimum threshold of values to use from data column (min visits or unique for example)
        :param reverse_dates: should data points be reversed, i.e test date against future data.
            default is False (test against past data)
        :param cp_threshold:
        :param model_base_dir: custom base dir from which model is taken
            (use calc_dir when training model to specify custom output dir)
        :param email_to: email to send test report to
        :param std_cp: std of cp for country alarm
        :param avg_cp: avg of cp for country alarm
        :param std_ss: std of significant slope for country alarm
        :param avg_ss: avg of significant slope for country alarm
        :param named_spark_args: named spark args dictionary to override defaults
        :param spark_configs: spark configs dictionary to override defaults
        :param spark_queue: override queue (default is 'calculation')
        :param benchmark_mode: specify whether this test is run from within model training (benchmark==True) or independently

        """
        countries_list = map(int, countries.split(','))

        input_paths = ConsistencyPaths.gen_input_paths(
            base_dir=self.ti.base_dir,
            path=input_path,
            base_date=self.ti.date,
            date_type=date_type
        )
        self.ti.set_s3_keys()

        def resolve_path(p):
            da = InputDataArtifact(self.ti, p, required_marker=False)
            return da.resolved_path

        # The model to use in the test
        model_base_dir = model_base_dir if model_base_dir is not None else self.ti.base_dir
        model_paths = ConsistencyPaths.gen_all_model_paths(
            base_dir=model_base_dir,
            name=test_name,
            model_date=model_date,
            countries=countries_list,
            date_type=date_type
        )
        base_model_path = ConsistencyPaths.gen_base_model_path(
            model_base_dir, test_name, model_date, date_type)
        for mp in model_paths:
            mp_data_artifact = InputDataArtifact(self.ti, mp, required_size=10, required_marker=True)
            base_model_path = ConsistencyPaths.extract_model_base_path_from_country_path(mp_data_artifact.resolved_path)

        # the benchmark result to use in the test (for comparison)
        benchmark_path = ConsistencyPaths.gen_model_benchmark_path(
            base_dir=model_base_dir,
            name=test_name,
            model_date=model_date,
            date_type=date_type
        )
        benchmark_path_data_artifact = InputDataArtifact(self.ti, benchmark_path, required_size=10, required_marker=True)

        # path for the model copy to save inside the test output
        model_for_test_base_path = ConsistencyPaths.gen_base_output_path(
            base_dir=self.ti.calc_dir,
            name=test_name,
            test_date=self.ti.date,
            path_type=ConsistencyOutputType.Model,
            date_type=date_type
        )

        model_for_test_base_da = OutputDataArtifact(self.ti, model_for_test_base_path, required_marker=False)

        country_result_base_path = ConsistencyPaths.gen_base_output_path(
            base_dir=self.ti.calc_dir,
            name=test_name,
            test_date=self.ti.date,
            path_type=ConsistencyOutputType.Countries,
            date_type=date_type
        )

        country_result_base_da = OutputDataArtifact(self.ti, country_result_base_path, required_marker=False)

        total_results_path = ConsistencyPaths.gen_output_path(
            base_dir=self.ti.calc_dir,
            name=test_name,
            test_date=self.ti.date,
            path_type=ConsistencyOutputType.Total,
            date_type=date_type
        )

        total_results_da = OutputDataArtifact(self.ti, total_results_path, required_marker=False)

        # benchmark paths clarification:
        # If this is not a benchmark mode run (meaning - a regular consistency test) then the benchmark data should be
        # taken from the benchmark path (using data artifact) and output should be written to total_results_path.
        # If this a is benchmark mode (meaning - run as part of the model) then there should be no benchmark data as
        # input, and the total output should written to the benchmark folder
        print('Running test %s with model %s' % (test_name, model_date))
        command_params = ConsistencyTestInfra._get_consistency_test_command_params(
            test_name=test_name,
            data_date=self.ti.date,
            countries_list=countries_list,
            email_to=email_to,
            base_model_path=base_model_path,
            model_for_test_base_path=model_for_test_base_da.resolved_path,
            country_result_base_path=country_result_base_da.resolved_path,
            total_results_path=total_results_da.resolved_path if not benchmark_mode else benchmark_path,
            benchmark_path=benchmark_path_data_artifact.resolved_path if not benchmark_mode else None,
            cp_threshold=cp_threshold,
            std_cp=std_cp,
            avg_cp=avg_cp,
            std_ss=std_ss,
            avg_ss=avg_ss,
            input_paths=map(resolve_path, input_paths),
            data_column=data_column,
            platform=platform,
            reverse_dates=reverse_dates,
            tsv_schema_table=tsv_schema_table,
            data_column_threshold=data_column_threshold,
            benchmark_mode=benchmark_mode,
        )


        # list all outputs for output assert and managed output dirs
        all_outputs = ConsistencyPaths.gen_all_output_paths(
            base_dir=self.ti.calc_dir,
            name=test_name,
            test_date=self.ti.date,
            date_type=date_type,
            countries=countries_list
        )
        if benchmark_mode:
            all_outputs = [benchmark_path]

        self.run_consistency_py_spark(
            main_py_file='consistency_test_driver.py',
            command_params=command_params,
            output_dirs=all_outputs,
            named_spark_args=named_spark_args,
            spark_configs=spark_configs,
            queue=spark_queue
        )

        # output checks
        self.ti.assert_output_validity(all_outputs, min_size_bytes=10, validate_marker=True)
