from pylib.tasks.data import DataArtifact, RangedDataArtifact
from enum import Enum

GB = 1024 ** 3
MB = 1024 ** 2
KB = 1024
EMPTY_STRING = ""


def path_join(path, *paths):
    if path[0] != '/':
        path = '/' + path
    if len(path) < 2:
        raise Exception("Path Component is to short.")
    full_path = path
    for path in paths:
        if not path:
            continue
        fixed_path = path
        if path[0] != "/":
            fixed_path = "/" + fixed_path
        if path[-1] == "/":
            fixed_path = fixed_path[:-1]
        if len(fixed_path) < 2:
            raise Exception("Path has to start with /")
        full_path += fixed_path
    return full_path


class AppsPathResolver(object):
    # Inner class start
    class AppPath(object):
        def __init__(self, ti, base_dir, main_path, required_size, required_marker, path_type, path_suffix=None):
            self.ti = ti
            self.base_dir = base_dir
            self.main_path = main_path
            self.full_base_path = path_join(self.base_dir, self.main_path)
            self.required_size = required_size
            self.required_marker = required_marker
            self.path_type = path_type
            self.path_suffix = path_suffix

        def __get_date_suffix_by_type(self):
            if self.path_type == "daily":
                return self.ti.year_month_day()
            elif self.path_type == "monthly":
                return self.ti.year_month()
            else:
                raise Exception("AppsPathResolver: unknown path type.")

        def get_data_artifact(self, date_suffix=None):
            date_suffix = date_suffix if date_suffix else self.__get_date_suffix_by_type()
            return DataArtifact(path_join(self.full_base_path, date_suffix, self.path_suffix),
                                required_size=self.required_size,
                                required_marker=self.required_marker)

        # Rerurn base path without date_suffix
        def get_base_path(self):
            return self.full_base_path

        def get_full_path(self):
            return path_join(self.get_base_path(), self.__get_date_suffix_by_type())

        def assert_output_validity(self):
            return self.ti.assert_output_validity(self.get_full_path(), min_size_bytes=self.required_size,

                                                  validate_marker=self.required_marker)
        # Inner class end

    def __init__(self, ti):
        self.ti = ti
        self.apps_paths = {
            # Daily
            'usage_patterns_estimate': {'main_path': "daily/estimate/usage-patterns", 'size': 400 * MB,
                                        'marker': True, 'path_type': "daily"},

            'apps_datapool': {'main_path': "daily/apps-datapool", 'size': 52 * GB,
                              'marker': True, 'path_type': "daily"},

            'app_country_source_agg': {'main_path': "daily/aggregations/aggKey=AppCountrySourceKey", 'size': 0.9 * GB,
                                       'marker': True, 'path_type': "daily"},

            'extractor_1001': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1001", 'size': 65 * GB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1003': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1003", 'size': 1.8 * GB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1005': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1005", 'size': 850 * MB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1009': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1009", 'size': 1.8 * GB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1010': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1010", 'size': 850 * MB,
                               'marker': True, 'path_type': "daily"},

            'grouping_1001_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1001", 'size': 65 * GB,
                                             'marker': True,
                                             'path_type': "daily"},

            'grouping_1003_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1003", 'size': 1.8 * GB,
                                             'marker': True,
                                             'path_type': "daily"},

            'grouping_1005_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1005", 'size': 950 * MB,
                                             'marker': True,
                                             'path_type': "daily"},

            'grouping_1009_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1009", 'size': 4.6 * GB,
                                             'marker': True,
                                             'path_type': "daily"},

            'grouping_1010_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1010", 'size': 186 * GB,
                                             'marker': True,
                                             'path_type': "daily"},

            'agg_app_country_delta_key': {'main_path': "daily/aggregations/aggKey=AppCountryDeltaKey",
                                          'size': 800 * MB,  # TODO update.
                                          'marker': True, 'path_type': "daily"},
            'agg_country_delta_key': {'main_path': "daily/aggregations/aggKey=CountryDeltaKey",
                                      'size': 1 * MB,  # TODO update.
                                      'marker': True, 'path_type': "daily"},

            'agg_app_country_source_day_hour': {'main_path': "daily/aggregations/aggKey=AppCountrySourceDayHourKey",
                                                'size': 120 * MB,
                                                'marker': True, 'path_type': "daily"},

            'agg_app_country_source_1009_key': {'main_path': "daily/aggregations/aggKey=AppCountrySource1009Key",
                                                'size': 950 * MB, 'marker': True,
                                                'path_type': "daily"},

            'agg_app_country_source_key': {'main_path': "daily/aggregations/aggKey=AppCountrySourceKey",
                                           'size': 950 * MB,
                                           'marker': True,
                                           'path_type': "daily"},

            'agg_country_source_1009_key': {'main_path': "daily/aggregations/aggKey=CountrySource1009Key",
                                            'size': 400 * KB,
                                            'marker': True,
                                            'path_type': "daily"},

            'agg_country_source_key': {'main_path': "daily/aggregations/aggKey=CountrySourceKey", 'size': 400 * KB,
                                       'marker': True,
                                       'path_type': "daily"},

            'agg_app_country_source_joined_key': {'main_path': "daily/aggregations/aggKey=AppCountrySourceJoinedKey",
                                                  'size': 7.4 * GB, 'marker': True,
                                                  'path_type': "daily"},

            'pre_estimate_app_country': {'main_path': "daily/pre-estimate/app-engagement/estkey=AppCountryKey",
                                         'size': 380 * MB, 'marker': True,
                                         'path_type': "daily"},
            'pre_estimate_1009_app_country': {'main_path': "daily/pre-estimate/app-engagement/estkey=AppCountry1009Key",
                                              'size': 380 * MB, 'marker': True,
                                              'path_type': "daily"},

            'time_series_estimation': {'main_path': "daily/time-series-weighted-predict",
                                       'size': 850 * MB, 'marker': True,
                                       'path_type': "daily"},

            'apps_for_analyze_decision': {'main_path': "daily/osm/apps_for_analyze_decision",
                                          'size': 9.5 * MB, 'marker': True,
                                          'path_type': "daily"},

            'app_engagement_estimation': {'main_path': "daily/estimate/app-engagement/estkey=AppCountryKey",
                                          'size': 570 * MB, 'marker': True,
                                          'path_type': "daily"},

            'real_numbers_adjustments_by_new_users': {
                'main_path': "monthly/android-real-numbers-v2/by-new-users/adjustments",
                'size': 1 * KB, 'marker': True,
                'path_type': "monthly"},

            'real_numbers_adjustments_by_active_users': {
                'main_path': "monthly/android-real-numbers-v2/by-active-users/adjustments",
                'size': 1 * KB, 'marker': True,
                'path_type': "monthly"},

            'app_downloads_alph': {
                'main_path': "daily/estimate/app-downloads-alph/estkey=AppCountryKey",
                'size': 95 * MB, 'marker': True,
                'path_type': "daily"},

            'app_engagement_realnumbers': {
                'main_path': "daily/estimate/app-engagement-realnumbers-tsv/estkey=AppCountryKey",
                'size': 1.5 * GB, 'marker': True,
                'path_type': "daily"},
            # Snapshot/Window
            'app_scores': {
                'main_path': "%(mode)s/app-scores" % {'mode': self.ti.mode},
                'size': 900 * MB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': "daily"}
        }

    def __get_base_dir(self, in_or_out, path_prefix):
        base_dir = self.ti.base_dir if in_or_out == "in" else self.ti.calc_dir
        return base_dir if not path_prefix else path_join(path_prefix, base_dir)

    def __get_android_apps_analytics_base(self, in_or_out, path_prefix):
        base_dir = self.__get_base_dir(in_or_out, path_prefix)
        return path_join(base_dir, "android-apps-analytics")

    def __get_mobile_analytics_base(self, in_or_out, path_prefix):
        base_dir = self.__get_base_dir(in_or_out, path_prefix)
        return path_join(base_dir, "mobile-analytics")

    def __create_app_path_object(self, base_dir, path_details, *args, **kwargs):
        return AppsPathResolver.AppPath(self.ti, base_dir, path_details['main_path'], path_details['size'],
                                        path_details['marker'], path_details['path_type'], *args, **kwargs)

    # Paths Getters
    def get_app_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_country_source_agg'], path_suffix)

    def get_extractor_1001(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1001'], path_suffix)

    def get_extractor_1003(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1003'], path_suffix)

    def get_extractor_1005(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1005'], path_suffix)

    def get_extractor_1009(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1009'], path_suffix)

    def get_extractor_1010(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1010'], path_suffix)

    def get_grouping_1001_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1001_report_parquet'], path_suffix)

    def get_grouping_1003_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1003_report_parquet'], path_suffix)

    def get_grouping_1005_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1005_report_parquet'], path_suffix)

    def get_grouping_1009_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1009_report_parquet'], path_suffix)

    def get_grouping_1010_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1010_report_parquet'], path_suffix)

    def get_agg_app_country_delta_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_app_country_delta_key'], path_suffix)

    def get_agg_country_delta_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_country_delta_key'], path_suffix)

    def get_agg_app_country_source_1009_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_app_country_source_1009_key'], path_suffix)

    def get_agg_app_country_source_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_app_country_source_key'], path_suffix)

    def get_agg_country_source_1009_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_country_source_1009_key'], path_suffix)

    def get_agg_country_source_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_country_source_key'], path_suffix)

    def get_agg_app_country_source_day_hour(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_app_country_source_day_hour'], path_suffix)

    def agg_app_country_source_joined_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_app_country_source_joined_key'], path_suffix)

    def get_pre_estimate_app_country(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['pre_estimate_app_country'], path_suffix)

    def get_pre_estimate_1009_app_country(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['pre_estimate_1009_app_country'], path_suffix)

    def get_time_series_estimation(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['time_series_estimation'], path_suffix)

    def get_apps_for_analyze_decision(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['apps_for_analyze_decision'], path_suffix)

    def get_app_engagement_estimation(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_engagement_estimation'], path_suffix)

    def get_real_numbers_adjustments_by_new_users(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['real_numbers_adjustments_by_new_users'], path_suffix)

    def get_real_numbers_adjustments_by_active_users(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['real_numbers_adjustments_by_active_users'], path_suffix)

    def get_app_downloads_alph(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_downloads_alph'], path_suffix)

    def get_app_engagement_realnumbers(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_engagement_realnumbers'], path_suffix)

    def get_apps_datapool(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['apps_datapool'], path_suffix)

    def get_usage_patterns_estimate(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_patterns_estimate'], path_suffix)
