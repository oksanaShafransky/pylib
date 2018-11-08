from datetime import date
from dateutil.relativedelta import relativedelta

from pylib.common.date_utils import get_dates_range
from pylib.consistency.consistency_types import ConsistencyDateType, ConsistencyOutputType
from pylib.hive.common import get_date_partition_path

OUTPUT_PATH_TYPE_TOTAL = "total"
OUTPUT_PATH_TYPE_MODEL = "model"
OUTPUT_PATH_TYPE_COUNTRY = "country"

MODEL_TYPE_COUNTRIES = "countries"
MODEL_TYPE_BENCHMARK = "benchmark"


class ConsistencyPaths(object):

    # base_dir: /similargroup/data/analytics/conistency

    has_day_partition = {
        ConsistencyDateType.DayOfWeek: True,
        ConsistencyDateType.Day: True,
        ConsistencyDateType.Month: False
    }

    @staticmethod
    def gen_base_model_path(base_dir, name, model_date, date_type):
        return '{base_dir}/consistency/{name}/model/{date_part}'.format(
            base_dir=base_dir,
            name=name,
            date_part=get_date_partition_path(
                model_date.year,
                model_date.month,
                model_date.day if ConsistencyPaths.has_day_partition[date_type] else None)
        )


    @staticmethod
    def extract_model_base_path_from_country_path(path):
        return path.split('/type=countries')[0]


    @staticmethod
    def gen_model_country_path(base_dir, name, model_date, country, date_type):
        return '{base_path}/type={model_type}/country={country}'.format(
            base_path=ConsistencyPaths.gen_base_model_path(base_dir, name, model_date, date_type),
            model_type=MODEL_TYPE_COUNTRIES,
            country=country
        )

    @staticmethod
    def gen_model_benchmark_path(base_dir, name, model_date, date_type):
        return '{base_path}/type={model_type}'.format(
            base_path=ConsistencyPaths.gen_base_model_path(base_dir, name, model_date, date_type),
            model_type=MODEL_TYPE_BENCHMARK
        )


    @staticmethod
    def gen_all_model_paths(base_dir, name, model_date, countries, date_type):
        return [
            ConsistencyPaths.gen_model_country_path(base_dir, name, model_date, country, date_type) for country in countries
        ]


    @staticmethod
    def gen_base_output_path(base_dir, name, test_date, path_type, date_type):
        return '{base_dir}/consistency/{name}/output/{date}/type={type}'.format(
            base_dir=base_dir,
            name=name,
            type=path_type,
            date=get_date_partition_path(
                test_date.year,
                test_date.month,
                test_date.day if ConsistencyPaths.has_day_partition[date_type] else None
            ),
        )

    @staticmethod
    def gen_output_path(base_dir, name, test_date, path_type, date_type, country=None):
        return '{base_path}{optional_country}'.format(
            base_path=ConsistencyPaths.gen_base_output_path(
                base_dir=base_dir,
                name=name,
                test_date=test_date,
                path_type=path_type,
                date_type=date_type,
            ),
            optional_country='/country=%s' % country if country else ''
        )

    @staticmethod
    def gen_all_output_paths(base_dir, name, test_date, date_type, countries):
        country_outputs = [
            ConsistencyPaths.gen_output_path(base_dir, name, test_date, ConsistencyOutputType.Countries, date_type, country) for country in countries
        ]

        model_outputs = [
            ConsistencyPaths.gen_output_path(base_dir, name, test_date, ConsistencyOutputType.Model, date_type, country) for country in countries
        ]

        total_output = ConsistencyPaths.gen_output_path(base_dir, name, test_date, ConsistencyOutputType.Total, date_type)

        return country_outputs + model_outputs + [total_output]

    @staticmethod
    def _gen_input_dates(base_date, date_type=ConsistencyDateType.Day, sample_count=5):
        if date_type == ConsistencyDateType.Month:
            return get_dates_range(base_date, sample_count, 'months')[::-1]
        elif date_type == ConsistencyDateType.DayOfWeek:
            # Take 5 day dates with week intervals
            return get_dates_range(base_date, sample_count, 'weeks')[::-1]
        elif date_type == ConsistencyDateType.Day:
            # Take 5 day dates with day intervals
            return get_dates_range(base_date, sample_count, 'days')[::-1]
        else:
            return []

    @staticmethod
    def gen_input_paths(base_dir, path, base_date, date_type):
        input_dates = ConsistencyPaths._gen_input_dates(base_date, date_type)

        def get_date_part(da):
            return get_date_partition_path(
                da.year % 100,  # two-digits year
                da.month,
                da.day if ConsistencyPaths.has_day_partition[date_type] else None
            )

        return ['{base_dir}/{path}/{date_part}'.format(
                    base_dir=base_dir,
                    path=path,
                    date_part=get_date_part(d)
                ) for d in input_dates]

