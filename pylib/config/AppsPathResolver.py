from pylib.tasks.input_data_artifact import InputDataArtifact, InputRangedDataArtifact
from pylib.tasks.output_data_artifact import OutputDataArtifact

GiB = 1024 ** 3
MiB = 1024 ** 2
KiB = 1024

KB = 1000
MB = KB ** 2
GB = KB ** 3

EMPTY_STRING = ""
SCRAPING_BASE_DIR = "/similargroup/scraping"


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
        def __init__(self, ti, base_dir, main_path, required_size, required_marker, path_type,
                     path_suffix=None, in_or_out='in'):
            self.ti = ti
            self.base_dir = base_dir
            self.main_path = main_path
            self.full_base_path = path_join(self.base_dir, self.main_path)
            self.required_size = required_size
            self.required_marker = required_marker
            self.path_type = path_type
            self.path_suffix = path_suffix
            self.in_or_out = in_or_out

        def __get_date_suffix_by_type(self):
            if self.path_type == "daily":
                return self.ti.year_month_day()
            elif self.path_type == "monthly":
                return self.ti.year_month()
            #when we want only base path with no suffix
            elif self.path_type == "base_path":
                return ""
            else:
                raise Exception("AppsPathResolver: unknown path type.")

        def get_data_artifact(self, date_suffix=None, **kwargs):
            date_suffix = date_suffix if date_suffix else self.__get_date_suffix_by_type()
            if self.in_or_out == 'in':
                return InputDataArtifact(self.ti, path_join(self.full_base_path, date_suffix, self.path_suffix),
                                         required_size=self.required_size,
                                         required_marker=self.required_marker,
                                         **kwargs)
            else:
                return OutputDataArtifact(self.ti, path_join(self.full_base_path, date_suffix, self.path_suffix),
                                          required_size=self.required_size,
                                          required_marker=self.required_marker,
                                          **kwargs)

        def get_ranged_data_artifact(self, dates):
            if self.in_or_out == 'out':
                raise Exception("AppsPathSolver - Output doesn't have ranged data artifact")
            return InputRangedDataArtifact(self.ti, self.full_base_path, dates,
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
            'countries-conf': {'main_path': "daily/countries-conf",
                                'size': 1,
                                'marker': True, 'path_type': "daily"},

            #APPS MATCHING
            'sanitized-app-info': {'main_path': "apps-matching/sanitized",
                                   'size': 800 * MiB,
                                   'marker': True, 'path_type': "daily"},
            'matching-candidates': {'main_path': "apps-matching/candidates",
                                    'size': 20 * MiB,
                                    'marker': True, 'path_type': "daily"},
            'matching-learning-set': {'main_path': "apps-matching/ls",
                                      'size': 4 * MiB,
                                      'marker': True, 'path_type': "daily"},
            'matching_manual_pairs': {
                'main_path': "static/apps-matching/manual-pairs_2021_08_01",
                'size': 1 * KiB,
                'marker': True, 'path_type': "base_path"},
            'matching-image-features-ios': {'main_path': "apps-matching/image-features-ios",
                                            'size': 20 * MiB,
                                            'marker': True, 'path_type': "daily"},
            'matching-image-features-android': {'main_path': "apps-matching/image-features-android",
                                            'size': 20 * MiB,
                                            'marker': True, 'path_type': "daily"},
            'matching-training-data': {'main_path': "apps-matching/train-data",
                                       'size': 4 * MiB,
                                       'marker': True, 'path_type': "daily"},
            'matching-classification-data': {'main_path': "apps-matching/classification-data",
                                             'size': 30 * MiB,
                                             'marker': True, 'path_type': "daily"},
            'matching-model': {'main_path': "apps-matching/model",
                               'size': 1 * KiB,
                               'marker': False, 'path_type': "daily"},
            'matching-predict': {'main_path': "apps-matching/predict",
                                 'size': 5 * MiB,
                                 'marker': True, 'path_type': "daily"},
            'matching-tests': {'main_path': "apps-matching/tests",
                                     'size': 5 * MiB,
                                     'marker': True, 'path_type': "daily"},

            # MONITORING
            'monitoring-dau-window': {'main_path': "apps-monitoring/dau/window",
                                  'size': 5 * MB,
                                  'marker': True, 'path_type': "daily"},
            'monitoring-dau-predict': {'main_path': "apps-monitoring/dau/predict",
                                  'size': 6 * MB,
                                  'marker': True, 'path_type': "daily"},
            'monitoring-dau-anomal-zscores': {'main_path': "apps-monitoring/dau/anomal/zScores",
                                          'size': 100 * KB,
                                          'marker': True, 'path_type': "daily"},
            'monitoring-dau-anomal-stats': {'main_path': "apps-monitoring/dau/anomal/stats",
                                          'size': 20 * KB,
                                          'marker': True, 'path_type': "daily"},
            'monitoring-reach-window': {'main_path': "apps-monitoring/reach/window",
                                      'size': 5 * MB,
                                      'marker': True, 'path_type': "daily"},
            'monitoring-reach-predict': {'main_path': "apps-monitoring/reach/predict",
                                       'size': 7 * MB,
                                       'marker': True, 'path_type': "daily"},
            'monitoring-reach-anomal-zscores': {'main_path': "apps-monitoring/reach/anomal/zScores",
                                              'size': 100 * KB,
                                              'marker': True, 'path_type': "daily"},
            'monitoring-reach-anomal-stats': {'main_path': "apps-monitoring/reach/anomal/stats",
                                            'size': 20 * KB,
                                            'marker': True, 'path_type': "daily"},

            # MAU
            'mau_feature2_agg': {
                'main_path': "monthly/mau/aggregations/aggkey=Feature2Key",
                'size': 1 * KiB, #TODO change
                'marker': True, 'path_type': "daily"},


            'mau_user_app_country_agg': {
                'main_path': "monthly/mau/aggregations/aggkey=UserAppCountry",
                'size': 1 * KiB,  # TODO change
                'marker': True, 'path_type': "daily"},

            'new_users_db': {'main_path': "daily/downloads/new_users/new_users_db", 'size': 1 * KiB,
                             'marker': False, 'path_type': "base_path"},#We Can't track size here in a good way.


            'downloads_sources_for_analyze': {'main_path': "daily/downloads/downloads-sources-for-analyze",
                                              'size': 1 * KiB,
                                              'marker': True, 'path_type': "daily"},

            'downloads_app_country_country_source_agg': {'main_path': "daily/downloads/aggregations/aggKey=AppCountryCountrySourceKey",
                                                         'size': 5.8 * GiB,
                                                         'marker': True, 'path_type': "daily"},

            'copy_downloads_app_country_country_source_agg': {'main_path': "daily/downloads/aggregations/copied/aggKey=AppCountryCountrySourceKey",
                                                              'size': 5.8 * GiB,
                                                              'marker': True, 'path_type': "daily"},

            'calc_downloads_app_country_country_source_agg': {'main_path': "daily/downloads/aggregations/new_calc/aggKey=AppCountryCountrySourceKey",
                                                              'size': 6 * GB,
                                                              'marker': True, 'path_type': "daily"},


            'downloads_app_country_delta_key_agg': {'main_path': "daily/downloads/aggregations/aggKey=AppCountryDeltaKey",
                                                    'size': 60 * MiB,
                                                    'marker': True, 'path_type': "daily"},

            'copy_downloads_app_country_delta_key_agg': {'main_path': "/daily/downloads/aggregations/copied/aggKey=AppCountryDeltaKey",
                                                    'size': 60 * MiB,
                                                    'marker': True, 'path_type': "daily"},

            'calc_downloads_app_country_delta_key_agg': {'main_path': "daily/downloads/aggregations/new_calc/aggKey=AppCountryDeltaKey/",
                                                    'size': 60 * MiB,
                                                    'marker': True, 'path_type': "daily"},


            'downloads_country_delta_key_agg': {
                'main_path': "daily/downloads/aggregations/aggKey=CountryDeltaKey",
                'size': 85 * KiB,
                'marker': True, 'path_type': "daily"},

            'copy_downloads_country_delta_key_agg': {
                'main_path': "daily/downloads/aggregations/copied/aggKey=CountryDeltaKey",
                'size': 85 * KiB,
                'marker': True, 'path_type': "daily"},

            'calc_downloads_country_delta_key_agg': {
                'main_path': "daily/downloads/aggregations/new_calc/aggKey=CountryDeltaKey",
                'size': 85 * KiB,
                'marker': True, 'path_type': "daily"},

            'downloads_prior': {
                'main_path': "daily/downloads/downloads-prior/aggKey=AppCountry",
                'size': 600 * MiB,
                'marker': True, 'path_type': "daily"},

            #dau
            'dau_sources_for_analyze': {'main_path': "daily/dau/dau-sources-for-analyze",
                                        'size': 1 * KiB,
                                        'marker': True, 'path_type': "daily"},

            'dau_android_11_factor': {'main_path': "daily/dau/android_11_factor",
                                           'size': 10 * MiB,
                                           'marker': True, 'path_type': "daily"},

            'dau_app_country_source_agg': {'main_path': "daily/dau/aggregations/aggKey=AppCountrySourceKey",
                                           'size': 600 * MiB,
                                           'marker': True, 'path_type': "daily"},


            'copy_dau_app_country_source_agg': {'main_path': "daily/dau/aggregations/copied/aggKey=AppCountrySourceKey",
                                                'size': 600 * MiB,
                                                'marker': True, 'path_type': "daily"},

            'calc_dau_app_country_source_agg': {'main_path': "daily/dau/aggregations/new_calc/aggKey=AppCountrySourceKey",
                                                'size': 300 * MiB,
                                                'marker': True, 'path_type': "daily"},

            'dau_country_source_agg': {'main_path': "daily/dau/aggregations/aggKey=CountrySourceKey",
                                       'size': 10 * KiB,
                                       'marker': True, 'path_type': "daily"},

            'copy_dau_country_source_agg': {'main_path': "daily/dau/aggregations/copied/aggKey=CountrySourceKey",
                                            'size': 80 * KiB,
                                            'marker': True, 'path_type': "daily"},

            'calc_dau_country_source_agg': {'main_path': "daily/dau/aggregations/new_calc/aggKey=CountrySourceKey",
                                            'size': 80 * KiB,
                                            'marker': True, 'path_type': "daily"},

            'dau_join_agg': {'main_path': "daily/dau/aggregations/aggKey=AppCountrySourceJoinedKey",
                             'size': 1 * GiB,
                             'marker': True, 'path_type': "daily"},

            'dau_calc_join_agg': {'main_path': "daily/dau/aggregations/new_calc/aggKey=AppCountrySourceJoinedKey",
                             'size': 1 * GiB,
                             'marker': True, 'path_type': "daily"},

            'dau_for_ptft': {'main_path': "daily/dau/pre-estimate/dau-for-ptft",
                                    'size': 230 * MB,
                                    'marker': True, 'path_type': "daily"},

            'new_users_for_ptft': {'main_path': "daily/downloads/pre-estimate/new-users-for-ptft",
                                    'size': 325 * MiB,
                                    'marker': True, 'path_type': "daily"},

            'reach_for_ptft': {'main_path': "daily/downloads/pre-estimate/reach-for-ptft",
                                    'size': 900 * MiB,
                                    'marker': True, 'path_type': "daily"},

            'installs_for_ptft': {'main_path': "daily/downloads/pre-estimate/installs-for-ptft",
                                    'size': 325 * MiB,
                                    'marker': True, 'path_type': "daily"},

            'dau_sqs_preliminary': {'main_path': "daily/dau/pre-estimate/sqs-preliminary",
                                    'size': 200 * MiB,
                                    'marker': True, 'path_type': "daily"},

            'sqs_calc': {'main_path': "daily/dau/pre-estimate/sqs-calc-weights",
                        'size': 250 * MiB,
                        'marker': True, 'path_type': "daily"},

            'dau_prior': {'main_path': "daily/dau/pre-estimate/engagement-prior",
                         'size': 200 * MiB,
                         'marker': True, 'path_type': "daily"},

            'dau_estimate': {'main_path': "daily/dau/estimate/estKey=AppContryKey",
                                    'size': 150 * MiB,
                                    'marker': True, 'path_type': "daily"},

            'dau_with_ww_estimate': {'main_path': "daily/dau-with-ww/estimate/estKey=AppCountryKey",
                             'size': 150 * MiB,
                             'marker': True, 'path_type': "daily"},

            'mau_embee_estimate': {'main_path': "monthly/mau/estimate-embee/estKey=AppContryKey",
                                   'size': 0 * MiB,
                                   'marker': True, 'path_type': "monthly"},

            'mau_weighted_embee_est': {'main_path': "monthly/mau/estimate-embee-weighted/estKey=AppContryKey",
                                       'size': 0 * MiB,
                                       'marker': True, 'path_type': "monthly"},

            'mau_pre_est': {'main_path': "snapshot/estimate/app-mau-dp/estkey=AppCountry",
                            'size': 0 * MiB,
                            'marker': True, 'path_type': "monthly"},

            'mau_final_est': {'main_path': "snapshot/estimate/app-mau-adjusted-to-dau-dp/estkey=AppCountry",
                              'size': 0 * MiB,
                              'marker': True, 'path_type': "monthly"},


            # Daily
            'sfa': {'main_path': "daily/sources-for-analyze", 'size': 1 * KiB,
                    'marker': True, 'path_type': "daily"},

            'usage_patterns_estimate': {'main_path': "daily/estimate/usage-patterns", 'size': 100 * MiB,
                                        'marker': True, 'path_type': "daily"},

            'apps_datapool': {'main_path': "daily/apps-datapool", 'size': 10 * GiB,
                              'marker': True, 'path_type': "daily"},

            # lspool_daily is actually monthly data, and the job itself is responsible for the daily partition
            'apps_lspool_daily': {'main_path': "daily/apps-lspool", 'size': 50 * MiB,
                                  'marker': True, 'path_type': "monthly"},

            'apps_lspool_monthly': {'main_path': "monthly/apps-lspool", 'size': 2 * MiB,
                                    'marker': True, 'path_type': "monthly"},

            'downloads_alpha_estimation': {'main_path': "daily/estimate/app-downloads-alph/estkey=AppCountryKey",
                                           'size': 10 * MiB,
                                           'marker': True, 'path_type': "daily"},#TODO Delete After 1.12.2020 release

            'new_user_alpha_estimation': {'main_path': "daily/downloads/new_users/estimation/app-downloads-alph/estkey=AppCountryKey",
                                          'size': 270 * MiB,
                                          'marker': True, 'path_type': "daily"},

            'new_user_features': {'main_path': "daily/downloads/new_users/features",
                                          'size': 150 * MiB,
                                          'marker': True, 'path_type': "daily"},

            'new_user_learning_set': {'main_path': "daily/downloads/new_users/learning_set",
                                          'size': 150 * MiB,
                                          'marker': True, 'path_type': "daily"},

            'installs_alpha_estimation': {
                'main_path': "daily/downloads/installs/estimation/app-downloads-alph/estkey=AppCountryKey",
                'size': 270 * MiB,
                'marker': True, 'path_type': "daily"},

            'installs_alpha_estimation_ww': {
                'main_path': "daily/downloads/installs/estimation/app-downloads-alpha-ww/estkey=AppCountryKey",
                'size': 10 * MiB,
                'marker': True, 'path_type': "daily"},

            'reach_estimation': {
                'main_path': "daily/downloads/installed-apps/estimation/reach/estkey=AppCountryKey",
                'size': 600 * MiB,
                'marker': True, 'path_type': "daily"},

            'reach_and_downloads': {
                'main_path': "daily/downloads/installs/estimation/reach_and_downloads/estkey=AppCountryKey",
                'size': 100 * MiB,
                'marker': True, 'path_type': "daily"},

            'reach_estimation_ww': {
                'main_path': "daily/downloads/installed-apps/estimation/reach-ww/estkey=AppCountryKey",
                'size': 100 * MiB,
                'marker': True, 'path_type': "daily"},

            'ww_store_downloads_fetch': {
                'main_path': "daily/downloads/store_downloads/raw_ww_store_fetch",
                'size': 85 * MiB,
                'marker': True, 'path_type': "daily"},

            'ww_store_download_country_population': {
                'main_path': "daily/downloads/store_downloads/static/country_pop_adj",
                'size': 2 * KiB,
                'marker': True, 'path_type': "daily"},

             'ww_store_download_panel_country_share_est_pre_factor': {
                'main_path': "daily/downloads/store_downloads/estimation/est-panel-country-share/pre-factor",
                'size': 120 * MiB,
                'marker': True, 'path_type': "daily"},

            'ww_store_download_panel_country_share_est': {
                'main_path': "daily/downloads/store_downloads/estimation/est-panel-country-share/estKey=AppCountryKey",
                'size': 120 * MiB,
                'marker': True, 'path_type': "daily"},

            'ww_store_download_app_delta': {
                'main_path': "daily/downloads/store_downloads/ww_downloads/ww_app_delta",
                'size': 1 * MiB,
                'marker': True, 'path_type': "daily"},

            'ww_store_download_weighted_download_est': {
                'main_path': "daily/downloads/store_downloads/estimation/app_weighted_ww_download_est/estKey=App",
                'size': 1 * KiB,
                'marker': True, 'path_type': "daily"},

            'store_download_country_adj_row': {
                'main_path': "daily/downloads/store_downloads/ROW_country_adjustment",
                'size': 1 * KiB,  # TODO Fix
                'marker': True, 'path_type': "daily"},

            'store_downloads_realnumbers_estimation': {
                'main_path': "daily/downloads/store_downloads/estimation/est-realnumbers",
                'size': 1 * KiB,  # TODO Fix
                'marker': True, 'path_type': "daily"},


            'app_country_source_agg': {'main_path': "daily/aggregations/aggKey=AppCountrySourceKey", 'size': 0.9 * GiB,
                                       'marker': True, 'path_type': "daily"},

            'extractor_1001': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1001", 'size': 7 * GiB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1003': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1003", 'size': 200 * MiB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1005': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1005", 'size': 150 * MiB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1005_on_server_side': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1005OnServerSide", 'size': 20 * MiB,
                               'marker': True, 'path_type': "daily"}, # TODO update real size

            'extractor_5555': {'main_path': "daily/extractors/extracted-metric-data/rtype=R5555", 'size': 50 * MiB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1008': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1008", 'size': 15 * MiB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1009': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1009", 'size': 100 * MiB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1010': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1010", 'size': 400 * MiB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1015': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1015", 'size': 60 * MiB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1019': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1019", 'size': 20 * MiB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1111': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1111", 'size': 50 * MiB,
                               'marker': True, 'path_type': "daily"},

            'extractor_bobble1001': {'main_path': "daily/extractors/extracted-metric-data/rtype=bobbleR1001",
                                     'size': 500 * MiB, 'marker': True, 'path_type': "daily"},

            'extractor_bobble1005': {'main_path': "daily/extractors/extracted-metric-data/rtype=bobbleR1005",
                                     'size': 10 * MiB, 'marker': True, 'path_type': "daily"},

            'extractor_bobble1008': {'main_path': "daily/extractors/extracted-metric-data/rtype=bobbleR1008",
                                     'size': 1 * GiB, 'marker': True, 'path_type': "daily"},

            'extractor_mfour1008': {'main_path': "daily/extractors/extracted-metric-data/rtype=mfourR1008",
                                    'size': 18 * MiB, 'marker': True, 'path_type': "daily"},

            'bobble_installed_apps': {'main_path': 'raw-s2s/bobble-installed-apps', 'size': 15 * GiB,
                                      'marker': False, 'path_type': "daily"},

            'bobble_apps_sessions': {'main_path': 'raw-s2s/bobble-apps-sessions', 'size': 3 * GiB,
                                     'marker': False, 'path_type': "daily"},

            'extractor_ga_daily': {'main_path': 'raw-s2s/extractor_ga_daily', 'size': 10 * MiB,
                                   'marker': True, 'path_type': "monthly"},

            'extractor_ga_monthly': {'main_path': 'raw-s2s/extractor_ga_monthly', 'size': 450 * KiB,
                                     'marker': True, 'path_type': "monthly"},

            'extractor_kwh_daily': {'main_path': 'raw-s2s/extractor_kwh_daily', 'size': 3 * MiB,
                                    'marker': True, 'path_type': "monthly"},

            'extractor_kwh_monthly': {'main_path': 'raw-s2s/extractor_kwh_monthly', 'size': 250 * KiB,
                                      'marker': True, 'path_type': "monthly"},

            'mfour_apps_sessions': {'main_path': 'raw-s2s/mfour-apps-sessions', 'size': 80 * MiB,
                                    'marker': False, 'path_type': "daily"},

            'embee_app_session': {'main_path': "raw-stats-embee/app_session", 'size': 10 * MiB,
                                  'marker': True, 'path_type': "daily"},

            'embee_device_info': {'main_path': "raw-stats-embee/device_info", 'size': 10 * MiB,
                                  'marker': True, 'path_type': "daily"},

            'embee_demographics': {'main_path': "raw-stats-embee/demographics", 'size': 3 * MiB,
                                   'marker': True, 'path_type': "daily"},

            'embee_naive_estimation': {'main_path': "raw-stats-embee/naive_estimation", 'size': 0 * MiB,
                                       'marker': True, 'path_type': "daily"},

            'embee_joined_data_output': {'main_path': "stats-mobile/parquet/rtype=R1111", 'size': 10 * MiB,
                                         'marker': True, 'path_type': "daily"},
            ###

            'grouping_1001_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1001", 'size': 20 * GiB,
                                             'marker': True,
                                             'path_type': "daily"},

            'grouping_1001_report_parquet_upsolver': {'main_path': "stats-mobile/parquet_adjusted/rtype=R1001", 'size': 16 * GiB,
                                             'marker': False,
                                             'path_type': "daily"},

            'grouping_1003_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1003", 'size': 200 * MiB,
                                             'marker': False,
                                             'path_type': "daily"},

            'grouping_1003_report_parquet_upsolver': {'main_path': "stats-mobile/parquet_adjusted/rtype=R1003", 'size': 1 * MiB,
                                                      'marker': False,
                                                      'path_type': "daily"},

            'grouping_1005_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1005", 'size': 400 * MiB,
                                             'marker': False, #TODO revert to True.
                                             'path_type': "daily"},

            'grouping_1005_report_parquet_upsolver': {'main_path': "stats-mobile/parquet_adjusted/rtype=R1005", 'size': 320 * MiB,
                                             'marker': False,
                                             'path_type': "daily"},

            'grouping_1008_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1008", 'size': 50 * MiB,
                                             'marker': True,
                                             'path_type': "daily"},

            'grouping_1008_report_parquet_upsolver': {'main_path': "stats-mobile/parquet_adjusted/rtype=R1008", 'size': 50 * MiB,
                                             'marker': False,
                                             'path_type': "daily"},

            'grouping_1009_report_parquet_upsolver': {'main_path': "stats-mobile/parquet_adjusted/rtype=R1009", 'size': 700 * MiB,
                                             'marker': True,
                                             'path_type': "daily"},

            'grouping_1009_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1009", 'size': 700 * MiB,
                                             'marker': True,
                                             'path_type': "daily"},

            'grouping_1010_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1010", 'size': 50 * GiB,
                                             'marker': True,
                                             'path_type': "daily"},
            'grouping_1015_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1015", 'size': 300 * MiB,
                                             'marker': True,
                                             'path_type': "daily"},

            'grouping_1016_report_parquet_upsolver': {'main_path': "stats-mobile/parquet_adjusted/rtype=R1016",
                                                      'size': 1 * MiB,  #FIXME
                                                      'marker': True,
                                                      'path_type': "daily"},

            'grouping_1111_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1111", 'size': 400 * MiB,
                                             'marker': True,
                                             'path_type': "daily"},

            'agg_app_country_source_days_back':{'main_path': "daily/retention/aggregations/aggKey=AppCountrySourceDaysbackKey",
                                                'size': 50 * MiB,
                                                'marker': True, 'path_type': "daily"},

            'agg_app_country_delta_key': {'main_path': "daily/aggregations/aggKey=AppCountryDeltaKey",
                                          'size': 600 * MiB,
                                          'marker': True, 'path_type': "daily"},

            'app_pairs_agg': {'main_path': "daily/aggregations/aggkey=AppPairCountryKey",
                              'size': 600 * MiB,  #TODO change.
                              'marker': True, 'path_type': "daily"},

            'agg_country_delta_key': {'main_path': "daily/aggregations/aggKey=CountryDeltaKey",
                                      'size': 120 * KiB,  # TODO update.
                                      'marker': True, 'path_type': "daily"},

            'agg_app_country_source_day_hour': {'main_path': "daily/aggregations/aggKey=AppCountrySourceDayHourKey",
                                                'size': 20 * MiB,
                                                'marker': True, 'path_type': "daily"},

            'agg_app_country_source_1009_key': {'main_path': "daily/aggregations/aggKey=AppCountrySource1009Key",
                                                'size': 950 * MiB, 'marker': True,
                                                'path_type': "daily"},

            'agg_app_country_source_key': {'main_path': "daily/aggregations/aggKey=AppCountrySourceKey",
                                           'size': 950 * MiB,
                                           'marker': True,
                                           'path_type': "daily"},

            'agg_country_source_1009_key': {'main_path': "daily/aggregations/aggKey=CountrySource1009Key",
                                            'size': 400 * KiB,
                                            'marker': True,
                                            'path_type': "daily"},

            'agg_country_source_key': {'main_path': "daily/aggregations/aggKey=CountrySourceKey", 'size': 400 * KiB,
                                       'marker': True,
                                       'path_type': "daily"},

            'agg_app_country_source_joined_key': {'main_path': "daily/aggregations/aggKey=AppCountrySourceJoinedKey",
                                                  'size': 3 * GiB, 'marker': True,
                                                  'path_type': "daily"},

            'pre_estimate_app_country': {'main_path': "daily/pre-estimate/app-engagement/estkey=AppCountryKey",
                                         'size': 380 * MiB, 'marker': True,
                                         'path_type': "daily"},
            'pre_estimate_1009_app_country': {'main_path': "daily/pre-estimate/app-engagement/estkey=AppCountry1009Key",
                                              'size': 380 * MiB, 'marker': True,
                                              'path_type': "daily"},

            'time_series_estimation': {'main_path': "daily/time-series-weighted-predict",
                                       'size': 650 * MiB, 'marker': True,
                                       'path_type': "daily"},

            'apps_for_analyze_decision': {'main_path': "daily/osm/apps_for_analyze_decision",
                                          'size': 9.5 * MiB, 'marker': True,
                                          'path_type': "daily"},

            'app_engagement_estimation': {'main_path': "daily/estimate/app-engagement/estkey=AppCountryKey",
                                          'size': 90 * MiB, 'marker': True,
                                          'path_type': "daily"},

            'real_numbers_adjustments_by_new_users': {
                'main_path': "monthly/android-real-numbers-v2/by-new-users/adjustments",
                'size': 1 * KiB, 'marker': True,
                'path_type': "monthly"},

            'real_numbers_adjustments_for_mau': {
                'main_path': "monthly/android-real-numbers-v2/for-mau/adjustments",
                'size': 1 * KiB, 'marker': True,
                'path_type': "monthly"},

            'real_numbers_adjustments_by_active_users': {
                'main_path': "monthly/android-real-numbers-v2/by-active-users/adjustments",
                'size': 1 * KiB, 'marker': True,
                'path_type': "monthly"},

            'real_numbers_adjustments_by_active_devices': {
                'main_path': "monthly/android-real-numbers-v2/by-active-devices/adjustments",
                'size': 1 * KiB, 'marker': True,
                'path_type': "monthly"},

            'system_apps': {
                'main_path': "daily/system-apps",
                'size': 30 * KiB, 'marker': True,
                'path_type': "daily"},

            'app_downloads_alph': {
                'main_path': "daily/estimate/app-downloads-alph/estkey=AppCountryKey",
                'size': 40 * MiB, 'marker': True,
                'path_type': "daily"},

            'os_share_1001': {
                'main_path': "daily/os_share/based=1001",
                'size': 1 * KiB, 'marker': True,
                'path_type': "daily"},

            'os_app_share_1001': {
                'main_path': "daily/os_app_share/based=1001",
                'size': 1 * KiB, 'marker': True,
                'path_type': "daily"},

            'app_engagement_realnumbers': {
                'main_path': "daily/estimate/app-engagement-realnumbers-tsv/estkey=AppCountryKey",
                'size': 900 * MiB, 'marker': True,
                'path_type': "daily"},
            'app_engagement_realnumbers_parquet': {
                'main_path': "daily/estimate/app-engagement-realnumbers/estkey=AppCountryKey",
                'size': 400 * MiB, 'marker': True,
                'path_type': "daily"},
            # Snapshot/WindowF
            'app_scores': {
                'main_path': "%(mode)s/app-scores" % {'mode': self.ti.mode},
                'size': 900 * MiB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': "daily"},


            'app_panel': {
                'main_path': "%s/estimate/app-panel/type=%s" % (self.ti.mode, self.ti.mode_type),
                'size': 1 * KiB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': "monthly"},

            'country_panel': {
                'main_path': "%s/estimate/country-panel/type=%s" % (self.ti.mode, self.ti.mode_type),
                'size': 1 * KiB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': "monthly"},

            'app_affinity': {'main_path': "%s/estimate/app-affinity-ww/type=%s" % (self.ti.mode, self.ti.mode_type),
                             'size': 90 * MiB, 'marker': True,
                             'path_type': "monthly"},

            #NSM TO OSM
            'preprocessed_1010': {'main_path': "daily/osm/preprocessed-1010",
                                  'size': 80 * GiB, 'marker': True,
                                  'path_type': "daily"},

            'preprocessed_1015': {'main_path': "daily/osm/preprocessed-1015",
                                  'size': 1 * KiB, 'marker': True,
                                  'path_type': "daily"}, #TODO fix

            'preprocessed_1008': {'main_path': "daily/osm_usage/preprocessed-1008",
                                  'size': 1 * KiB, 'marker': True,
                                  'path_type': "daily"},  # TODO fix

            'osm_ip_model': {'main_path': "monthly/osm-ip-model",
                             'size': 400 * MiB, 'marker': True,
                             'path_type': "monthly"},

            'domain_resolved_1010': {'main_path': "daily/osm/domain-resolved-1010",
                                     'size': 50 * GiB, 'marker': True,
                                     'path_type': "daily"},

            'base_dataset': {'main_path': "daily/osm/base-dataset",
                             'size': 3 * MiB, 'marker': True,
                             'path_type': "daily"},

            'usage_base_dataset': {'main_path': "daily/osm_usage/base-dataset",
                                   'size': 1 * KiB, 'marker': True,
                                   'path_type': "daily"},

            'domain_fg': {'main_path': "daily/osm/domain-fg",
                          'size': 400 * KiB, 'marker': True,
                          'path_type': "daily"},# TODO fix

            'usage_domain_fg': {'main_path': "daily/osm_usage/domain-fg",
                                'size': 400 * KiB, 'marker': True,
                                'path_type': "daily"},  # TODO fix

            'osm_features': {'main_path': "daily/osm/features",
                             'size': 1 * MiB, 'marker': True,
                             'path_type': "daily"},  # TODO fix

            'usage_osm_features': {'main_path': "daily/osm_usage/features",
                                   'size': 1 * MiB, 'marker': True,
                                   'path_type': "daily"},  # TODO fix

            'nsm_model': {'main_path': "daily/osm/nsm_model",
                          'size': 1 * KiB, 'marker': False,
                          'path_type': "daily"},  # TODO fix

            'usage_osm_model': {'main_path': "daily/osm_usage/usage_osm_model",
                                'size': 1 * KiB, 'marker': False,
                                'path_type': "daily"},  # TODO fix

            'nsm_to_osm_predictions': {'main_path': "daily/osm/predictions",
                                       'size': 1 * KiB, 'marker': True,
                                       'path_type': "daily"}, # TODO fix

            'usage_to_osm_predictions': {'main_path': "daily/osm_usage/predictions",
                                         'size': 1 * KiB, 'marker': True,
                                         'path_type': "daily"},  # TODO fix

            'nsm_test_dataset': {'main_path': "daily/osm/test_dataset",
                                 'size': 1 * KiB, 'marker': True,
                                 'path_type': "daily"},  # TODO fix

            'usage_test_dataset': {'main_path': "daily/osm_usage/test_dataset",
                                   'size': 1 * KiB, 'marker': True,
                                   'path_type': "daily"},  # TODO fix

            'osm_predictions_not_fixed': {'main_path': "daily/osm/predictions_not_fixed",
                                          'size': 1 * KiB, 'marker': True,
                                          'path_type': "daily"},  # TODO fix

            'ga': {'main_path': "daily/apps-lspool",
                   'size': 1 * KiB, 'marker': False,
                   'path_type': "daily"},  # TODO fix

            #SCRAPING
            'app_info': {'main_path': "mobile/app-info",
                                   'size': 1 * GiB, 'marker': True,
                                   'path_type': "daily"},

            # store-analysis
            'google_play_version_db': {'main_path': "google-play/app_version_db",
                         'size': 100 * MiB, 'marker': False,
                         'path_type': "base_path"},

            'ios_app_store_version_db': {'main_path': "iOS-app-store/app_version_db",
                                       'size': 100 * MiB, 'marker': False,
                                       'path_type': "base_path"},

            # Static paths
            'countries_full_names': {'main_path': "resources/country-codes-dict",
                                       'size': 1 * KiB, 'marker': False,
                                       'path_type': "base_path"},
            'z_norm_dist': {'main_path': "static/norm_dist",
                                     'size': 1 * KiB, 'marker': True,
                                     'path_type': "base_path"},

        }

    def __get_base_dir(self, in_or_out, path_prefix):
        base_dir = self.ti.base_dir if in_or_out == "in" else self.ti.calc_dir
        return base_dir if not path_prefix else path_join(path_prefix, base_dir)

    def __get_android_apps_analytics_base(self, in_or_out, path_prefix):
        base_dir = self.__get_base_dir(in_or_out, path_prefix)
        return path_join(base_dir, "android-apps-analytics")

    def __get_store_analytics_base(self, in_or_out, path_prefix):
        base_dir = self.__get_base_dir(in_or_out, path_prefix)
        return path_join(base_dir, "store-analytics")

    def __get_mobile_analytics_base(self, in_or_out, path_prefix):
        base_dir = self.__get_base_dir(in_or_out, path_prefix)
        return path_join(base_dir, "mobile-analytics")

    def __get_scraping_base(self, path_prefix):
        #Only similagroup/data dirs working with ti base and calc dir.
        base_dir = SCRAPING_BASE_DIR
        return base_dir if not path_prefix else path_join(path_prefix, base_dir)

    def __create_app_path_object(self, base_dir, path_details, *args, **kwargs):
        return AppsPathResolver.AppPath(self.ti, base_dir, path_details['main_path'], path_details['size'],
                                        path_details['marker'], path_details['path_type'], *args, **kwargs)

    # Paths Getters
    def get_app_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_country_source_agg'], path_suffix, in_or_out)

    def get_extractor_1001(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1001'], path_suffix, in_or_out)

    def get_extractor_1003(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1003'], path_suffix, in_or_out)

    def get_extractor_1005(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1005'], path_suffix, in_or_out)

    def get_extractor_1005_on_server_side(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1005_on_server_side'], path_suffix, in_or_out)

    def get_extractor_5555(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_5555'], path_suffix, in_or_out)

    def get_extractor_1008(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1008'], path_suffix, in_or_out)

    def get_extractor_1009(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1009'], path_suffix, in_or_out)

    def get_extractor_1010(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1010'], path_suffix, in_or_out)

    def get_extractor_1015(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1015'], path_suffix, in_or_out)

    def get_extractor_1019(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1019'], path_suffix, in_or_out)

    def get_extractor_1111(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_1111'], path_suffix, in_or_out)

    def get_extractor_bobble1001(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_bobble1001'], path_suffix, in_or_out)

    def get_extractor_bobble1005(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_bobble1005'], path_suffix, in_or_out)

    def get_extractor_bobble1008(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_bobble1008'], path_suffix, in_or_out)

    def get_extractor_mfour1008(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_mfour1008'], path_suffix, in_or_out)

    def get_bobble_installed_apps(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['bobble_installed_apps'], path_suffix, in_or_out)

    def get_bobble_apps_sessions(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['bobble_apps_sessions'], path_suffix, in_or_out)

    def get_extractor_ga_daily(self, in_or_out, path_prefix=None, path_suffix=None):
            return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                                 self.apps_paths['extractor_ga_daily'], path_suffix, in_or_out)

    def get_extractor_ga_monthly(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_ga_monthly'], path_suffix, in_or_out)

    def get_extractor_kwh_daily(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_kwh_daily'], path_suffix, in_or_out)

    def get_extractor_kwh_monthly(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_kwh_monthly'], path_suffix, in_or_out)

    def get_mfour_apps_sessions(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['mfour_apps_sessions'], path_suffix, in_or_out)

    def get_embee_app_session(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['embee_app_session'], path_suffix, in_or_out)

    def get_embee_device_info(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['embee_device_info'], path_suffix, in_or_out)

    def get_embee_demographics(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['embee_demographics'], path_suffix, in_or_out)

    def get_embee_joined_data_output(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['embee_joined_data_output'], path_suffix, in_or_out)

    def get_embee_naive_estimation(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['embee_naive_estimation'], path_suffix, in_or_out)
    ###

    def get_grouping_1001_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1001_report_parquet'], path_suffix, in_or_out)

    def get_grouping_1001_report_parquet_upsolver(self, in_or_out, path_prefix=None, path_suffix="is_valid=true"):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1001_report_parquet_upsolver'], path_suffix, in_or_out)

    def get_grouping_1003_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1003_report_parquet'], path_suffix, in_or_out)

    def get_grouping_1003_report_parquet_upsolver(self, in_or_out, path_prefix=None, path_suffix="is_valid=true"):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1003_report_parquet_upsolver'], path_suffix, in_or_out)

    def get_grouping_1005_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1005_report_parquet'], path_suffix, in_or_out)

    def get_grouping_1005_report_parquet_upsolver(self, in_or_out, path_prefix=None, path_suffix="is_valid=true"):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1005_report_parquet_upsolver'], path_suffix, in_or_out)

    def get_grouping_1008_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1008_report_parquet'], path_suffix, in_or_out)

    def get_grouping_1008_report_parquet_upsolver(self, in_or_out, path_prefix=None, path_suffix="is_valid=true"):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1008_report_parquet_upsolver'], path_suffix, in_or_out)

    def get_grouping_1009_report_parquet_upsolver(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1009_report_parquet_upsolver'], path_suffix, in_or_out)

    def get_grouping_1009_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1009_report_parquet'], path_suffix, in_or_out)

    def get_grouping_1010_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1010_report_parquet'], path_suffix, in_or_out)

    def get_grouping_1015_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1015_report_parquet'], path_suffix, in_or_out)

    def get_grouping_1016_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1016_report_parquet_upsolver'], path_suffix, in_or_out)

    def get_grouping_1111_report_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['grouping_1111_report_parquet'], path_suffix, in_or_out)

    def get_agg_app_country_delta_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_app_country_delta_key'], path_suffix, in_or_out)

    def get_app_pairs_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_pairs_agg'], path_suffix, in_or_out)

    def get_sfa(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['sfa'], path_suffix, in_or_out)

    def get_agg_country_delta_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_country_delta_key'], path_suffix, in_or_out)

    def get_agg_app_country_source_1009_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_app_country_source_1009_key'], path_suffix, in_or_out)

    def get_agg_app_country_source_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_app_country_source_key'], path_suffix, in_or_out)

    def get_agg_country_source_1009_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_country_source_1009_key'], path_suffix, in_or_out)

    def get_agg_country_source_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_country_source_key'], path_suffix, in_or_out)

    def get_agg_app_country_source_day_hour(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_app_country_source_day_hour'], path_suffix, in_or_out)

    def get_agg_app_country_source_days_back(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_app_country_source_days_back'], path_suffix, in_or_out)

    def agg_app_country_source_joined_key(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['agg_app_country_source_joined_key'], path_suffix, in_or_out)

    def get_pre_estimate_app_country(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['pre_estimate_app_country'], path_suffix, in_or_out)

    def get_est_app_country_source_days_back(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['est_app_country_source_days_back'], path_suffix, in_or_out)

    def get_pre_estimate_1009_app_country(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['pre_estimate_1009_app_country'], path_suffix, in_or_out)

    def get_time_series_estimation(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['time_series_estimation'], path_suffix, in_or_out)

    def get_apps_for_analyze_decision(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['apps_for_analyze_decision'], path_suffix, in_or_out)

    def get_app_engagement_estimation(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_engagement_estimation'], path_suffix, in_or_out)

    def get_real_numbers_adjustments_by_new_users(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['real_numbers_adjustments_by_new_users'], path_suffix, in_or_out)

    def get_real_numbers_adjustments_for_mau(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['real_numbers_adjustments_for_mau'], path_suffix, in_or_out)

    def get_real_numbers_adjustments_by_active_users(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['real_numbers_adjustments_by_active_users'], path_suffix, in_or_out)

    def get_real_numbers_adjustments_by_active_devices(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['real_numbers_adjustments_by_active_devices'], path_suffix, in_or_out)

    def get_system_apps(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['system_apps'], path_suffix, in_or_out)

    def get_app_downloads_alph(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_downloads_alph'], path_suffix, in_or_out)

    def get_os_share_1001(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['os_share_1001'], path_suffix, in_or_out)

    def get_os_app_share_1001(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['os_app_share_1001'], path_suffix, in_or_out)

    def get_app_engagement_realnumbers(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_engagement_realnumbers'], path_suffix, in_or_out)

    def get_app_engagement_realnumbers_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_engagement_realnumbers_parquet'], path_suffix, in_or_out)

    def get_apps_datapool(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['apps_datapool'], path_suffix, in_or_out)

    def get_apps_lspool_daily(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['apps_lspool_daily'], path_suffix, in_or_out)

    def get_apps_lspool_monthly(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['apps_lspool_monthly'], path_suffix, in_or_out)

    def get_usage_patterns_estimate(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_patterns_estimate'], path_suffix, in_or_out)

    def get_downloads_alpha_estimation(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['downloads_alpha_estimation'], path_suffix, in_or_out)

    def get_new_user_alpha_estimation(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['new_user_alpha_estimation'], path_suffix, in_or_out)

    def get_new_users_features(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['new_user_features'], path_suffix, in_or_out)

    def get_new_users_learning_set(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['new_user_learning_set'], path_suffix, in_or_out)

    def get_installs_alpha_estimation(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['installs_alpha_estimation'], path_suffix, in_or_out)

    def get_installs_alpha_estimation_ww(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['installs_alpha_estimation_ww'], path_suffix, in_or_out)

    def get_reach_estimation(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['reach_estimation'], path_suffix, in_or_out)

    def get_reach_and_downloads(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['reach_and_downloads'], path_suffix, in_or_out)

    def get_reach_estimation_ww(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['reach_estimation_ww'], path_suffix, in_or_out)

    def get_ww_store_downloads_fetch(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ww_store_downloads_fetch'], path_suffix, in_or_out)

    def get_ww_store_download_country_population(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ww_store_download_country_population'], path_suffix, in_or_out)

    def get_ww_store_download_panel_country_share_est_pre_factor(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ww_store_download_panel_country_share_est_pre_factor'], path_suffix, in_or_out)

    def get_ww_store_download_panel_country_share_est(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ww_store_download_panel_country_share_est'], path_suffix, in_or_out)

    def get_ww_store_download_app_delta(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ww_store_download_app_delta'], path_suffix, in_or_out)

    def get_ww_store_download_weighted_download_est(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ww_store_download_weighted_download_est'], path_suffix, in_or_out)

    def get_store_download_country_adj_row(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['store_download_country_adj_row'], path_suffix, in_or_out)

    def get_store_downloads_realnumbers_estimation(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['store_downloads_realnumbers_estimation'], path_suffix, in_or_out)

    def get_preprocessed_1010(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['preprocessed_1010'], path_suffix, in_or_out)

    def get_preprocessed_1015(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['preprocessed_1015'], path_suffix, in_or_out)

    def get_preprocessed_1008(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['preprocessed_1008'], path_suffix, in_or_out)

    def get_osm_ip_model(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['osm_ip_model'], path_suffix, in_or_out)

    def get_domain_resolved_1010(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['domain_resolved_1010'], path_suffix, in_or_out)

    def get_base_dataset(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['base_dataset'], path_suffix, in_or_out)

    def get_usage_base_dataset(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_base_dataset'], path_suffix, in_or_out)

    def get_domain_fg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['domain_fg'], path_suffix, in_or_out)

    def get_usage_domain_fg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_domain_fg'], path_suffix, in_or_out)

    def get_osm_features(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['osm_features'], path_suffix, in_or_out)

    def get_usage_osm_features(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_osm_features'], path_suffix, in_or_out)

    def get_nsm_model(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['nsm_model'], path_suffix, in_or_out)

    def get_usage_osm_model(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_osm_model'], path_suffix, in_or_out)

    def get_nsm_to_osm_predictions(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['nsm_to_osm_predictions'], path_suffix, in_or_out)

    def get_usage_to_osm_predictions(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_to_osm_predictions'], path_suffix, in_or_out)

    def get_nsm_test_dataset(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['nsm_test_dataset'], path_suffix, in_or_out)

    def get_usage_test_dataset(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_test_dataset'], path_suffix, in_or_out)

    #Temp
    def get_osm_predictions_not_fixed(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['osm_predictions_not_fixed'], path_suffix, in_or_out)

    def get_new_users_db(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['new_users_db'], path_suffix, in_or_out)

    def get_downloads_sfa(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['downloads_sources_for_analyze'], path_suffix, in_or_out)

    def get_downloads_app_country_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['downloads_app_country_country_source_agg'], path_suffix, in_or_out)

    def get_downloads_copy_app_country_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
            return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                                 self.apps_paths['copy_downloads_app_country_country_source_agg'], path_suffix, in_or_out)

    def get_downloads_calc_app_country_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['calc_downloads_app_country_country_source_agg'], path_suffix, in_or_out)


    def get_downloads_app_country_delta_key_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['downloads_app_country_delta_key_agg'], path_suffix, in_or_out)

    def get_downloads_copy_app_country_delta_key_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['copy_downloads_app_country_delta_key_agg'], path_suffix, in_or_out)

    def get_downloads_calc_app_country_delta_key_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['calc_downloads_app_country_delta_key_agg'], path_suffix, in_or_out)


    def get_downloads_country_delta_key_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['downloads_country_delta_key_agg'], path_suffix, in_or_out)

    def get_downloads_copy_country_delta_key_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['copy_downloads_country_delta_key_agg'], path_suffix, in_or_out)

    def get_downloads_calc_country_delta_key_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['calc_downloads_country_delta_key_agg'], path_suffix, in_or_out)


    def get_downloads_prior(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['downloads_prior'], path_suffix, in_or_out)


    def get_countries_conf(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['countries-conf'], path_suffix, in_or_out)

    # apps matching
    def get_sanitized_app_info(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['sanitized-app-info'], path_suffix, in_or_out)
    def get_matching_candidates(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-candidates'], path_suffix, in_or_out)
    def get_matching_learning_set(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-learning-set'], path_suffix, in_or_out)
    def get_matching_manual_pairs(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching_manual_pairs'], path_suffix, in_or_out)
    def get_matching_image_features_ios(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-image-features-ios'], path_suffix, in_or_out)
    def get_matching_image_features_android(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-image-features-android'], path_suffix, in_or_out)
    def get_matching_training_data(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-training-data'], path_suffix, in_or_out)
    def get_matching_classification_data(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-classification-data'], path_suffix, in_or_out)
    def get_matching_model(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-model'], path_suffix, in_or_out)
    def get_matching_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-predict'], path_suffix, in_or_out)
    def get_matching_tests(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-tests'], path_suffix, in_or_out)

    # apps monitoring
    def get_monitoring_dau_window(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-dau-window'], path_suffix, in_or_out)
    def get_monitoring_dau_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-dau-predict'], path_suffix, in_or_out)
    def get_monitoring_dau_anomal_zscores(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-dau-anomal-zscores'], path_suffix, in_or_out)
    def get_monitoring_dau_anomal_stats(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-dau-anomal-stats'], path_suffix, in_or_out)
    def get_monitoring_reach_window(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-reach-window'], path_suffix, in_or_out)
    def get_monitoring_reach_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-reach-predict'], path_suffix, in_or_out)
    def get_monitoring_reach_anomal_zscores(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-reach-anomal-zscores'], path_suffix, in_or_out)
    def get_monitoring_reach_anomal_stats(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-reach-anomal-stats'], path_suffix, in_or_out)

    # dau
    def get_dau_sfa(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['dau_sources_for_analyze'], path_suffix, in_or_out)

    def get_dau_android_11_factor(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['dau_android_11_factor'], path_suffix, in_or_out)

    def get_dau_app_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['dau_app_country_source_agg'], path_suffix, in_or_out)

    def get_dau_copy_app_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['copy_dau_app_country_source_agg'], path_suffix, in_or_out)

    def get_dau_calc_app_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['calc_dau_app_country_source_agg'], path_suffix, in_or_out)

    def get_dau_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['dau_country_source_agg'], path_suffix, in_or_out)

    def get_dau_calc_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['calc_dau_country_source_agg'], path_suffix, in_or_out)

    def get_dau_copy_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['copy_dau_country_source_agg'], path_suffix, in_or_out)

    def get_dau_app_country_source_join_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['dau_join_agg'], path_suffix, in_or_out)

    def get_dau_calc_app_country_source_join_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['dau_calc_join_agg'], path_suffix, in_or_out)

    def get_sqs_preliminary(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['dau_sqs_preliminary'], path_suffix, in_or_out)

    def get_dau_for_ptft(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['dau_for_ptft'], path_suffix, in_or_out)

    def get_reach_for_ptft(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['reach_for_ptft'], path_suffix, in_or_out)

    def get_new_users_for_ptft(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['new_users_for_ptft'], path_suffix, in_or_out)

    def get_installs_for_ptft(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['installs_for_ptft'], path_suffix, in_or_out)

    def get_sqs_calc(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['sqs_calc'], path_suffix, in_or_out)

    def get_dau_prior(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['dau_prior'], path_suffix, in_or_out)

    def get_dau_final_est(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['dau_estimate'], path_suffix, in_or_out)

    def get_dau_with_ww_est(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['dau_with_ww_estimate'], path_suffix, in_or_out)

    #MAU
    def get_mau_user_app_country_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_user_app_country_agg'], path_suffix, in_or_out)

    def get_mau_feature2_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_feature2_agg'], path_suffix, in_or_out)

    def get_mau_est(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_final_est'], path_suffix, in_or_out)

    def get_mau_embee_est(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_embee_estimate'], path_suffix, in_or_out)

    def get_mau_weighted_embee_est(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_weighted_embee_est'], path_suffix, in_or_out)

    def get_mau_pre_est(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_pre_est'], path_suffix, in_or_out)
    #dau

    def get_ga(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ga'], path_suffix, in_or_out)

    #SCRAPING
    def get_android_app_info(self, in_or_out, path_prefix=None, path_suffix="store=0"):
        return self.__create_app_path_object(self.__get_scraping_base(path_prefix),
                                             self.apps_paths['app_info'], path_suffix, in_or_out)

    def get_ios_app_info(self, in_or_out, path_prefix=None, path_suffix="store=1"):
        return self.__create_app_path_object(self.__get_scraping_base(path_prefix),
                                             self.apps_paths['app_info'], path_suffix, in_or_out)

    # Store analysis
    def get_google_play_version_db(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_version_db'], path_suffix, in_or_out)
    
    def get_ios_app_store_version_db(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_version_db'], path_suffix, in_or_out)

    # Static Paths
    def get_countries_full_names(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['countries_full_names'], path_suffix, in_or_out)
    def get_z_norm_dist(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['z_norm_dist'], path_suffix, in_or_out)

    def get_app_panel(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_panel'], path_suffix, in_or_out)

    def get_country_panel(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['country_panel'], path_suffix, in_or_out)

    def get_app_affinity(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_affinity'], path_suffix, in_or_out)
