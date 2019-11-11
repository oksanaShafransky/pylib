import path
from pylib.tasks.data import DataArtifact, RangedDataArtifact


class AppsPathResolver(object):
    def __init__(self, ti):
        self.ti = ti

    apps_paths = {
        'app_country_source_agg': {'main_path': "daily/aggregations/aggKey=AppCountrySourceKey", 'size': 1e9, 'marker': True, 'path_type': "daily"},
        'extractor_1009': {'main_path': "daily/extractors/extracted-metric-data/rtype=1009", 'size': 2e9, 'marker': True, 'path_type': "daily"},
        'grouping_1009_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1009", 'size': 5e9, 'marker': True, 'path_type': "daily"}
    }

    class AppPath(object):
        def __init__(self, ti, base_dir, main_path, required_size, required_marker, path_type):
            self.ti = ti
            self.base_dir = base_dir
            self.main_path = main_path
            self.full_base_path = path.join(self.base_dir, self.main_path)
            self.required_size = required_size
            self.required_marker = required_marker
            self.path_type = path_type

        def __get_date_suffix_by_type(self):
            if self.path_type == "daily":
                return self.ti.year_month_day()
            else:
                raise Exception("AppsPathResolver: unknown path type.")

        def get_data_artifact(self, date_suffix=None):
            date_suffix = date_suffix if date_suffix else self.__get_date_suffix_by_type()
            return DataArtifact(path.join(self.full_base_path, date_suffix), required_size=self.required_size, required_marker=self.required_marker)

        #Rerurn base path without date_suffix
        def get_base_path(self):
            return self.full_base_path


    def __get_base_dir(self, in_or_out):
        return self.ti.base_dir if in_or_out == "in" else self.ti.calc_dir

    def __get_android_apps_analytics_base(self, in_or_out):
        base_dir = self.__get_base_dir(in_or_out)
        return path.join(base_dir, "android-apps-analytics")

    def __create_app_path_object(self, base_dir, path_details):
        return AppsPathResolver.AppPath(self.ti, base_dir, path_details['main_path'], path_details['size'], path_details['marker'], path_details['path_type'])

    def get_app_country_source_agg(self, in_or_out):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out), AppsPathResolver.apps_paths['app_country_source_agg'])

    def get_grouping_1009_report_parquet(self, in_or_out):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out), AppsPathResolver.apps_paths['grouping_1009_report_parquet'])















