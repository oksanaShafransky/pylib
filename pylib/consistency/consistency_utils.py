from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import pylib.hive.common as common
from pylib.tasks.ptask_infra import TasksInfra


class ConsistencyTestInfra(object):
    def __init__(self, ti):
        self.ti = ti

    @staticmethod
    def gen_model_paths(base_dir, name, model_date, has_day_partition, countries_list):
        return ['%(base_dir)s/consistency/model/%(name)s/country=%(country)s/%(date_part)s' %
                {
                    'base_dir': base_dir,
                    'name': name,
                    'country': country,
                    'date_part': common.get_date_partition_path(model_date.year % 100, model_date.month,
                                                                model_date.day if has_day_partition else 1)
                } for country in countries_list]

    @staticmethod
    def gen_result_paths(base_dir, name, test_date, countries_list, has_day_partition):
        return ['%(base_dir)s/consistency/output/%(name)s/bycountry/country=%(country)s/%(date)s' %
                {
                    'base_dir': base_dir,
                    'name': name,
                    'country': country,
                    'date': common.get_date_partition_path(test_date.year % 100, test_date.month,
                                                           test_date.day if has_day_partition else None)
                } for country in countries_list]

    @staticmethod
    def gen_grouped_result_path(base_dir, name, test_date, has_day_partition):
        return '%(base_dir)s/consistency/output/%(name)s/grouped/%(date)s' % \
               {
                   'base_dir': base_dir,
                   'name': name,
                   'date': common.get_date_partition_path(test_date.year % 100, test_date.month,
                                                          test_date.day if has_day_partition else None)
               }

    @staticmethod
    def gen_input_dates(base_date, date_type='day', sample_count=5):
        if date_type == 'month':
            return [date((base_date-relativedelta(months=x)).year,
                         (base_date-relativedelta(months=x)).month, 1) for x in range(0, sample_count)]
        elif date_type == 'week':
            return [base_date-relativedelta(weeks=x) for x in range(0, sample_count)]
        elif date_type == 'day':
            [base_date-relativedelta(days=x) for x in range(0, sample_count)]
        else:
            return []


    @staticmethod
    def gen_input_paths(base_dir, path, base_date, date_type):
        return ['%(base_dir)s%(path)s/%(date_part)s' %
                {
                    'base_dir': base_dir,
                    'path': path,
                    'date_part': common.get_date_partition_path(d.year % 100, d.month,
                                                                d.day if date_type is not 'month' else None)

                } for d in ConsistencyTestInfra.gen_input_dates(base_date, date_type)]

    @staticmethod
    def get_model_training_command_params(name,
                                          data_date,
                                          collector_type,
                                          source_db,
                                          source_table,
                                          data_column,
                                          has_day_partition,
                                          countries,
                                          reverse_dates=None,
                                          collector_threshold=100000,
                                          apps_rank='0-2000',
                                          first_column=None,
                                          num_features=2,
                                          sig_noise_coeff=0.01,
                                          std_cp=0.036,
                                          avg_cp=0.0586,
                                          std_ss=0.05,
                                          avg_ss=0.22,
                                          tv_gauss=2.6,
                                          tv_symm=1.0):
        params = {
            't': name,
            'dt': data_date,
            'c': countries,
            'ct': collector_type,
            'sdb': source_db,
            'sta': source_table,
            'dc': data_column,
            'rv': reverse_dates,
            'r': apps_rank,
            'col_thr': collector_threshold,
            'dp': has_day_partition,
            'fc': first_column,
            'nf': num_features,
            'snr': sig_noise_coeff,
            'scp': std_cp,
            'acp': avg_cp,
            'sss': std_ss,
            'ass': avg_ss,
            'tvg': tv_gauss,
            'tvs': tv_symm
        }

        return params

    def run_consistency_test_py_spark(self, command_params, spark_args=None):
        main_py_file = 'consistency_test_driver.py'
        self.run_consistency_py_spark(main_py_file, command_params, spark_args)

    def run_consistency_model_py_spark(self, command_params, spark_args=None):
        main_py_file = 'consistency_model_driver.py'
        self.run_consistency_py_spark(main_py_file, command_params, spark_args)

    def run_consistency_py_spark(self, main_py_file, command_params, spark_args=None):
        self.ti.run_py_spark(
            app_name='Consistency Test For %s' % command_params['t'],
            main_py_file=main_py_file,
            command_params=command_params,
            module='consistency',
            queue=spark_args['queue'] if 'queue' in spark_args else 'calculation',
            files=['/etc/hive/conf/hive-site.xml'],
            py_files=[
                self.ti.execution_dir + '/consistency/consistency-0.0.0.dev0-py2.7.egg',
                self.ti.execution_dir + '/sw-spark-common/sw_spark-0.0.0.dev0-py2.7.egg'
            ],
            named_spark_args={
                'num-executors': spark_args['num-executors'] if 'num-executors' in spark_args else 200,
                'executor-memory': spark_args['executor-memory'] if 'executor-memory' in spark_args else '4G',
                'driver-memory': spark_args['driver-memory'] if 'driver-memory' in spark_args else '10G',
                'master': spark_args['master'] if 'master' in spark_args else 'yarn-cluster'
            },
            spark_configs={'conf': 'spark.yarn.executor.memoryOverhead=1024'},
            use_bigdata_defaults=True,
        )

    @staticmethod
    def get_latest_model_date(name):
        key = 'services/consistency/model/%s' % name
        d = TasksInfra.kv().get(key)
        print('got %s from key %s' % (d, key))
        return d

    def train_model(self,
                    name,
                    collector_type,
                    source_db,
                    source_table,
                    data_column,
                    input_path,
                    has_day_partition,
                    date_type,
                    countries,
                    reverse_dates=None,
                    collector_threshold=100000,
                    apps_rank='0-2000',
                    first_column=None,
                    num_features=2,
                    sig_noise_coeff=0.01,
                    std_cp=0.036,
                    avg_cp=0.0586,
                    std_ss=0.05,
                    avg_ss=0.22,
                    tv_gauss=2.6,
                    tv_symm=1.0,
                    spark_args=dict()):

        input_paths = ConsistencyTestInfra.gen_input_paths(
            base_dir=self.ti.base_dir,
            path=input_path,
            base_date=self.ti.date,
            date_type=date_type
        )
        self.ti.assert_input_validity(input_paths, validate_marker=True)

        command_params = ConsistencyTestInfra.get_model_training_command_params(name=name,
                                                                                data_date=self.ti.date,
                                                                                collector_type=collector_type,
                                                                                source_db=source_db,
                                                                                source_table=source_table,
                                                                                data_column=data_column,
                                                                                has_day_partition=has_day_partition,
                                                                                countries=countries,
                                                                                reverse_dates=reverse_dates,
                                                                                collector_threshold=collector_threshold,
                                                                                apps_rank=apps_rank,
                                                                                first_column=first_column,
                                                                                num_features=num_features,
                                                                                sig_noise_coeff=sig_noise_coeff,
                                                                                std_cp=std_cp,
                                                                                avg_cp=avg_cp,
                                                                                std_ss=std_ss,
                                                                                avg_ss=avg_ss,
                                                                                tv_gauss=tv_gauss,
                                                                                tv_symm=tv_symm)

        countries_list = map(int, countries.split(','))
        self.run_consistency_model_py_spark(command_params, spark_args)

        # output checks
        model_paths = ConsistencyTestInfra.gen_model_paths(
            base_dir=self.ti.base_dir,
            name=name,
            has_day_partition=has_day_partition,
            model_date=self.ti.date,
            countries_list=countries_list
        )
        self.ti.assert_output_validity(model_paths, min_size_bytes=10, validate_marker=True)

    @staticmethod
    def get_consistency_test_command_params(name,
                                            data_date,
                                            collector_type,
                                            source_db,
                                            source_table,
                                            data_column,
                                            has_day_partition,
                                            countries_list,
                                            reverse_dates=None,
                                            apps_rank='0-2000',
                                            first_column=None,
                                            cp_threshold=0.90,
                                            model_date=None,
                                            email_to=None):

        model_date_to_use = ConsistencyTestInfra.get_latest_model_date(name) \
            if not model_date else model_date.strftime('%Y-%m-%d')

        print('Running test %s with model %s' % (name, model_date_to_use))
        params = {
            't': name,
            'dt': data_date,
            'c': ','.join(map(str, countries_list)),
            'ct': collector_type,
            'sdb': source_db,
            'sta': source_table,
            'dc': data_column,
            'rv': reverse_dates,
            'r': apps_rank,
            'dp': has_day_partition,
            'fc': first_column,
            'cp_thr': cp_threshold,
            'emt': email_to,
            'md': model_date_to_use
        }

        return params

    def test(self,
             name,
             collector_type,
             source_db,
             source_table,
             data_column,
             has_day_partition,
             countries,
             reverse_dates=None,
             apps_rank='0-2000',
             first_column=None,
             cp_threshold=0.90,
             model_date=None,
             email_to=None,
             spark_args=dict()):

        countries_list = map(int, countries.split(','))
        model_date = ConsistencyTestInfra.get_latest_model_date(name) \
            if not model_date else model_date
        model_date_parsed = datetime.strptime(model_date, '%Y-%m-%d')


        # input checks
        model_paths = ConsistencyTestInfra.gen_model_paths(
            base_dir=self.ti.base_dir,
            name=name,
            model_date=model_date_parsed,
            has_day_partition=has_day_partition,
            countries_list=countries_list
        )
        self.ti.assert_input_validity(model_paths, min_size_bytes=10, validate_marker=True)

        command_params = ConsistencyTestInfra.get_consistency_test_command_params(
            name=name,
            data_date=self.ti.date,
            collector_type=collector_type,
            source_db=source_db,
            source_table=source_table,
            data_column=data_column,
            has_day_partition=has_day_partition,
            countries_list=countries_list,
            reverse_dates=reverse_dates,
            apps_rank=apps_rank,
            first_column=first_column,
            cp_threshold=cp_threshold,
            model_date=model_date_parsed,
            email_to=email_to
        )

        self.run_consistency_test_py_spark(command_params, spark_args)

        # output checks
        result_paths = ConsistencyTestInfra.gen_result_paths(
            base_dir=self.ti.base_dir,
            name=name,
            test_date=self.ti.date,
            countries_list=countries_list,
            has_day_partition=has_day_partition
        )
        self.ti.assert_output_validity(result_paths, min_size_bytes=100, validate_marker=True)
        grouped_result_path = ConsistencyTestInfra.gen_grouped_result_path(
            base_dir=self.ti.base_dir,
            name=name,
            test_date=self.ti.date,
            has_day_partition=has_day_partition
        )
        self.ti.assert_output_validity(grouped_result_path, min_size_bytes=100, validate_marker=True)