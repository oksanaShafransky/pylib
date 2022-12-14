from pylib.tasks.input_data_artifact import InputDataArtifact, InputRangedDataArtifact, DEFAULT_SUFFIX_FORMAT
from pylib.tasks.output_data_artifact import OutputDataArtifact
from datetime import date
import copy

GiB = 1024 ** 3
MiB = 1024 ** 2
KiB = 1024

KB = 1000
MB = KB ** 2
GB = KB ** 3

EMPTY_STRING = ""
SCRAPING_BASE_DIR = "/similargroup/scraping"

GOOGLE_PLAY = '0'
IOS_APP_STORE = '1'


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
            if self.path_type == "daily" or self.path_type == "last-28" :
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

        def get_ranged_data_artifact(self, dates, suffix_format=DEFAULT_SUFFIX_FORMAT):
            if self.in_or_out == 'out':
                raise Exception("AppsPathSolver - Output doesn't have ranged data artifact")
            return InputRangedDataArtifact(self.ti, self.full_base_path, dates, suffix_format,
                                           required_size=self.required_size,
                                           required_marker=self.required_marker)

        def get_latest_success_data_artifact(self, lookback=None, **kwargs):
            """
            Returns the most recent data artifact with a _SUCCESS marker
            :param lookback: lookback value, timedelta is determined by ti.mode (daily, weekly, snapshot, etc.)
            :param kwargs: kwargs for the InputDataArtifact object
            :return: InputDataArtifact of the most recent path within the lookback range with a _SUCCESS marker
            """
            if self.in_or_out == 'out':
                raise Exception("AppsPathSolver - Output doesn't have latest success data artifact")
            self.ti.set_aws_credentials()
            for path, _ in reversed(self.ti.dates_range_paths(self.full_base_path + '/', lookback)):
                # Bad practice
                # if there had been a more specific exception for failure to locate data,
                # we could've used it instead as the except argument
                try:
                    data_artifact = InputDataArtifact(self.ti, path_join(path, self.path_suffix),
                                                      required_size=self.required_size,
                                                      required_marker=self.required_marker,
                                                      **kwargs)
                except Exception:
                    continue

                return data_artifact

            raise Exception("InputDataArtifact - Couldn't locate success markers for: %s in any of the datasources" % self.main_path)

        def get_model_data_artifact(self, version_suffix=None, **kwargs):
            """
            Returns an InputDataArtifact of a model.
            :param version_suffix:
            :param kwargs: kwargs for the InputDataArtifact object
            :return:
            """
            if self.in_or_out == 'out' or self.path_type != 'base_path':
                raise Exception("AppsPathSolver - Not a valid model path")
            return InputDataArtifact(self.ti, path_join(self.full_base_path, self.path_suffix, version_suffix),
                                     required_size=self.required_size,
                                     required_marker=self.required_marker,
                                     **kwargs)

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


            #Atlanta Project
            'aa_android_engagement': {'main_path': "android/estimation/engagement",
                                   'size': 1 * MiB,
                                   'marker': True, 'path_type': "monthly"},

            'aa_ios_engagement': {'main_path': "ios/estimation/engagement",
                                      'size': 1 * MiB,
                                      'marker': True, 'path_type': "monthly"},

            'aa_android_d30_retention': {'main_path': "android/estimation/retention-d30",
                                      'size': 500 * KiB,
                                      'marker': True, 'path_type': "monthly"},

            'aa_ios_d30_retention': {'main_path': "ios/estimation/retention-d30",
                                  'size': 200 * KiB,
                                  'marker': True, 'path_type': "monthly"},

            'aa_android_cross_affinity': {'main_path': "android/estimation/cross-affinity",
                                         'size': 1 * MiB,
                                         'marker': True, 'path_type': "monthly"},

            'aa_ios_cross_affinity': {'main_path': "ios/estimation/cross-affinity",
                                     'size': 1 * MiB,
                                     'marker': True, 'path_type': "monthly"},

            #APPS MATCHING
            'sanitized-app-info': {'main_path': "apps-matching/sanitized",
                                   'size': 800 * MiB,
                                   'marker': True, 'path_type': "daily"},
            'matching-candidates': {'main_path': "apps-matching/candidates",
                                    'size': 20 * MiB,
                                    'marker': True, 'path_type': "daily"},
            'matching-learning-set': {'main_path': "apps-matching/ls",
                                      'size': 3 * MB,
                                      'marker': True, 'path_type': "daily"},
            'matching_manual_pairs': {
                'main_path': "static/apps-matching/manual-pairs_2021_08_01",
                'size': 1 * KiB,
                'marker': True, 'path_type': "base_path"},
            'matching-image-features-ios': {'main_path': "apps-matching/image-features-ios",
                                            'size': 200 * MB,
                                            'marker': True, 'path_type': "daily"},
            'matching-image-features-android': {'main_path': "apps-matching/image-features-android",
                                            'size': 300 * MB,
                                            'marker': True, 'path_type': "daily"},
            'matching-hash-image-features-ios': {'main_path': "apps-matching/hash-image-features-ios",
                                            'size': 50 * MB,
                                            'marker': True, 'path_type': "daily"},
            'matching-hash-image-features-android': {'main_path': "apps-matching/hash-image-features-android",
                                                'size': 70 * MB,
                                                'marker': True, 'path_type': "daily"},
            'matching-language-features-ios': {'main_path': "apps-matching/language-features-ios",
                                            'size': 7 * MB,
                                            'marker': True, 'path_type': "daily"},
            'matching-language-features-android': {'main_path': "apps-matching/language-features-android",
                                                'size': 20 * MB,
                                                'marker': True, 'path_type': "daily"},
            'matching-training-data': {'main_path': "apps-matching/train-data",
                                       'size': 6.5 * MB,
                                       'marker': True, 'path_type': "daily"},
            'matching-train-data': {'main_path': "apps-matching/train",
                                       'size': 7 * MB,
                                       'marker': True, 'path_type': "daily"},
            'matching-test-data': {'main_path': "apps-matching/test",
                                       'size': 3 * MB,
                                       'marker': True, 'path_type': "daily"},
            'matching-classification-data': {'main_path': "apps-matching/classification-data",
                                             'size': 30 * MiB,
                                             'marker': True, 'path_type': "daily"},
            'matching-model': {'main_path': "apps-matching/model",
                               'size': 1 * KiB,
                               'marker': False, 'path_type': "daily"},
            'matching-raw-predict': {'main_path': "apps-matching/predict-raw",
                                 'size': 21 * MB,
                                 'marker': True, 'path_type': "daily"},
            'matching-predict-sanitized': {'main_path': "apps-matching/predict-sanitized",
                                         'size': 19 * MiB,
                                         'marker': True, 'path_type': "daily"},
            'matching-predict-consistency': {'main_path': "apps-matching/predict-consistency",
                                           'size': 19 * MiB,
                                           'marker': True, 'path_type': "daily"},
            'matching-predict': {'main_path': "apps-matching/predict",
                                 'size': 19 * MB,
                                 'marker': True, 'path_type': "daily"},
            'matching-stats-candidates-coverage': {'main_path': "apps-matching/stats/candidates/coverage",
                               'size': 1 * KB,
                               'marker': True, 'path_type': "daily"},
            'matching-stats-model': {'main_path': "apps-matching/stats/model",
                                                   'size': 1 * KB,
                                                   'marker': True, 'path_type': "daily"},
            'matching-stats-prediction-coverage': {'main_path': "apps-matching/stats/prediction/coverage",
                               'size': 1 * KB,
                               'marker': True, 'path_type': "daily"},
            'matching-stats-prediction-accuracy': {'main_path': "apps-matching/stats/prediction/accuracy",
                                                   'size': 1 * KB,
                                                   'marker': True, 'path_type': "daily"},
            'matching-stats-prediction-manual-accuracy': {'main_path': "apps-matching/stats/prediction-manual/accuracy",
                                                          'size': 1 * KB,
                                                          'marker': True, 'path_type': "daily"},
            'matching-stats-prediction-top-ls-accuracy': {'main_path': "apps-matching/stats/prediction-ls-top/accuracy",
                                                          'size': 1 * KB,
                                                          'marker': True, 'path_type': "daily"},
            'matching-rds': {'main_path': "apps-matching/rds",
                                                          'size': 1 * KB,
                                                          'marker': True, 'path_type': "daily"},

            # MONITORING
            'monitoring-dau-window': {'main_path': "apps-monitoring/dau/window",
                                  'size': 3.75 * MB,
                                  'marker': True, 'path_type': "daily"},
            'monitoring-open-rate-window': {'main_path': "apps-monitoring/open_rate/window",
                                      'size': 3.75 * MB,
                                      'marker': True, 'path_type': "daily"},
            'monitoring-downloads-window': {'main_path': "apps-monitoring/downloads/window",
                                        'size': 60 * KB,
                                        'marker': True, 'path_type': "daily"},
            'monitoring-reach-window': {'main_path': "apps-monitoring/reach/window",
                                        'size': 5 * MB,
                                        'marker': True, 'path_type': "daily"},
            'monitoring-unique-installs-window': {'main_path': "apps-monitoring/uniqueinstalls/window",
                                        'size': 2.5 * MB,
                                        'marker': True, 'path_type': "daily"},
            'monitoring-sessions-window': {'main_path': "apps-monitoring/sessions/window",
                                        'size': 4 * MB,
                                        'marker': True, 'path_type': "daily"},
            'monitoring-usagetime-window': {'main_path': "apps-monitoring/usagetime/window",
                                        'size': 4.5 * MB,
                                        'marker': True, 'path_type': "daily"},
            'monitoring-retention-window': {'main_path': "apps-monitoring/retention/window",
                                            'size': 300 * KB,
                                            'marker': True, 'path_type': "daily"},
            'monitoring-dau-predict': {'main_path': "apps-monitoring/dau/predict",
                                      'size': 5.25 * MB,
                                      'marker': True, 'path_type': "daily"},
            'monitoring-open-rate-predict': {'main_path': "apps-monitoring/open_rate/predict",
                                       'size': 5.25 * MB,
                                       'marker': True, 'path_type': "daily"},
            'monitoring-downloads-predict': {'main_path': "apps-monitoring/downloads/predict",
                                         'size': 100 * KB,
                                         'marker': True, 'path_type': "daily"},
            'monitoring-reach-predict': {'main_path': "apps-monitoring/reach/predict",
                                         'size': 7 * MB,
                                         'marker': True, 'path_type': "daily"},
            'monitoring-unique-installs-predict': {'main_path': "apps-monitoring/uniqueinstalls/predict",
                                         'size': 4.5 * MB,
                                         'marker': True, 'path_type': "daily"},
            'monitoring-sessions-predict': {'main_path': "apps-monitoring/sessions/predict",
                                         'size': 6 * MB,
                                         'marker': True, 'path_type': "daily"},
            'monitoring-usagetime-predict': {'main_path': "apps-monitoring/usagetime/predict",
                                         'size': 6.5 * MB,
                                         'marker': True, 'path_type': "daily"},
            'monitoring-retention-predict': {'main_path': "apps-monitoring/retention/predict",
                                             'size': 450 * KB,
                                             'marker': True, 'path_type': "daily"},
            'monitoring-dau-anomal-zscores': {'main_path': "apps-monitoring/dau/anomal/zScores",
                                          'size': 100 * KB,
                                          'marker': True, 'path_type': "daily"},
            'monitoring-open-rate-anomal-zscores': {'main_path': "apps-monitoring/open_rate/anomal/zScores",
                                              'size': 100 * KB,
                                              'marker': True, 'path_type': "daily"},
            'monitoring-downloads-anomal-zscores': {'main_path': "apps-monitoring/downloads/anomal/zScores",
                                                'size': 35 * KB,
                                                'marker': True, 'path_type': "daily"},
            'monitoring-reach-anomal-zscores': {'main_path': "apps-monitoring/reach/anomal/zScores",
                                                'size': 100 * KB,
                                                'marker': True, 'path_type': "daily"},
            'monitoring-unique-installs-anomal-zscores': {'main_path': "apps-monitoring/uniqueinstalls/anomal/zScores",
                                                'size': 100 * KB,
                                                'marker': True, 'path_type': "daily"},
            'monitoring-retention-anomal-zscores': {'main_path': "apps-monitoring/retention/anomal/zScores",
                                                'size': 0.4 * KB,
                                                'marker': True, 'path_type': "daily"},
            'monitoring-retention-anomal-countries': {'main_path': "apps-monitoring/retention/anomal/countries",
                                              'size': 20 * KB,
                                              'marker': True, 'path_type': "daily"},
            'monitoring-sessions-anomal-zscores': {'main_path': "apps-monitoring/sessions/anomal/zScores",
                                                'size': 100 * KB,
                                                'marker': True, 'path_type': "daily"},
            'monitoring-usagetime-anomal-zscores': {'main_path': "apps-monitoring/usagetime/anomal/zScores",
                                                'size': 100 * KB,
                                                'marker': True, 'path_type': "daily"},
             'monitoring-mau-window': {'main_path': "apps-monitoring/mau/window",
                                      'size': 3 * MB,
                                      'marker': True, 'path_type': "monthly"},
            'monitoring-mau-predict': {'main_path': "apps-monitoring/mau/predict",
                                      'size': 6 * MB,
                                      'marker': True, 'path_type': "monthly"},
            'monitoring-mau-anomal-zscores': {'main_path': "apps-monitoring/mau/anomal/zScores",
                                      'size': 0.5 * KB,
                                      'marker': True, 'path_type': "monthly"},
            'monitoring-mau-anomal-countries': {'main_path': "apps-monitoring/mau/anomal/countries",
                                      'size': 0.5 * KB,
                                      'marker': True, 'path_type': "monthly"},

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
            'downloads_intermediate_sources_for_analyze': {'main_path': "daily/downloads/downloads-intermediate-sources-for-analyze",
                                              'size': 1 * KiB,
                                              'marker': True, 'path_type': "daily"},

            'downloads_app_country_country_source_agg': {'main_path': "daily/downloads/aggregations/aggKey=AppCountryCountrySourceKey",
                                                         'size': 5.8 * GiB,
                                                         'marker': True, 'path_type': "daily"},

            'copy_downloads_app_country_country_source_agg': {'main_path': "daily/downloads/aggregations/copied/aggKey=AppCountryCountrySourceKey",
                                                              'size': 5.8 * GiB,
                                                              'marker': True, 'path_type': "daily"},

            'calc_downloads_app_country_country_source_agg': {'main_path': "daily/downloads/aggregations/new_calc/aggKey=AppCountryCountrySourceKey",
                                                              'size': 4 * GB,
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
                                            'size': 10 * KiB,
                                            'marker': True, 'path_type': "daily"},

            'dau_join_agg': {'main_path': "daily/dau/aggregations/aggKey=AppCountrySourceJoinedKey",
                             'size': 1 * GiB,
                             'marker': True, 'path_type': "daily"},

            'dau_calc_join_agg': {'main_path': "daily/dau/aggregations/new_calc/aggKey=AppCountrySourceJoinedKey",
                             'size': 1 * GB,
                             'marker': True, 'path_type': "daily"},

            'dau_for_ptft': {'main_path': "daily/dau/pre-estimate/dau-for-ptft",
                                    'size': 200 * MB,
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

            'ww_store_download_for_ptft': {'main_path': "daily/downloads/pre-estimate/ww_store_download_for_ptft",
                                  'size': 240 * MiB,  'marker': True, 'path_type': "daily"},




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

            'mau_dau_adjusted_estimation_rn': {'main_path': "monthly/mau/estimation/dau-mau-adjusted-rn",
                                   'size': 1 * MiB, #TODO fix
                                   'marker': True, 'path_type': "monthly"},

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

            'mau_predict_with_ww': {'main_path': "snapshot/estimate/app-mau-dau-with-ww/estkey=AppCountry",
                              'size': 0 * MiB,
                              'marker': True, 'path_type': "monthly"},

            'mau_android_factors': {'main_path': "snapshot/estimate/app-mau-factors",
                              'size': 1 * KB,
                              'marker': True, 'path_type': "monthly"},


            # Usage
            'usage_agg_app_country_before_mobidays_fix': {'main_path': "daily/usage/pre-agg",
                             'size': 10 * MB,
                             'marker': True, 'path_type': "daily"},
            'usage_agg_app_country_after_mobidays_fix': {'main_path': "daily/usage/agg",
                                      'size': 10 * MB,
                                      'marker': True, 'path_type': "daily"},
            'usage_prior': {'main_path': "daily/usage/prior",
                            'size': 11 * MB,
                            'marker': True, 'path_type': "daily"},
            'usage_estimation': {'main_path': "daily/usage/estimation",
                                 'size': 13 * MB,
                                 'marker': True, 'path_type': "daily"},
            'usage_estimation_ww': {'main_path': "daily/usage/ww",
                                 'size': 15 * MB,
                                 'marker': True, 'path_type': "daily"},

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
                                          'size': 250 * MiB,
                                          'marker': True, 'path_type': "daily"},

            'new_user_features': {'main_path': "daily/downloads/new_users/features",
                                          'size': 150 * MiB,
                                          'marker': True, 'path_type': "daily"},

            'new_user_learning_set': {'main_path': "daily/downloads/new_users/learning_set",
                                          'size': 150 * MiB,
                                          'marker': True, 'path_type': "daily"},

            'installs_alpha_estimation': {
                'main_path': "daily/downloads/installs/estimation/app-downloads-alph/estkey=AppCountryKey",
                'size': 250 * MiB,
                'marker': True, 'path_type': "daily"},

            'installs_alpha_estimation_ww': {
                'main_path': "daily/downloads/installs/estimation/app-downloads-alpha-ww/estkey=AppCountryKey",
                'size': 10 * MiB,
                'marker': True, 'path_type': "daily"},

            'reach_estimation': {
                'main_path': "daily/downloads/installed-apps/estimation/reach/estkey=AppCountryKey",
                'size': 550 * MiB,
                'marker': True, 'path_type': "daily"},

            'reach_and_downloads': {
                'main_path': "daily/downloads/installs/estimation/reach_and_downloads/estkey=AppCountryKey",
                'size': 10 * MiB,
                'marker': True, 'path_type': "daily"},

            'reach_estimation_ww': {
                'main_path': "daily/downloads/installed-apps/estimation/reach-ww/estkey=AppCountryKey",
                'size': 100 * MiB,
                'marker': True, 'path_type': "daily"},

            'ww_store_downloads_fetch': {
                'main_path': "daily/downloads/store_downloads/raw_ww_store_fetch",
                'size': 86 * MB,
                'marker': True, 'path_type': "daily"},

            'ww_store_download_country_population': {
                'main_path': "daily/downloads/store_downloads/static/country_pop_adj",
                'size': 2 * KiB,
                'marker': True, 'path_type': "base_path"},

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

            'extractor_1001': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1001", 'size': 3.5 * GB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1003': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1003", 'size': 200 * MiB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1005': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1005", 'size': 32 * MB,
                               'marker': True, 'path_type': "daily"},

            'extractor_1005_on_server_side': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1005OnServerSide", 'size': 20 * MiB,
                               'marker': True, 'path_type': "daily"}, # TODO update real size

            'extractor_5555': {'main_path': "daily/extractors/extracted-metric-data/rtype=R5555", 'size': 110 * MB,
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

            'extractor_1111': {'main_path': "daily/extractors/extracted-metric-data/rtype=R1111", 'size': 100 * MB,
                               'marker': True, 'path_type': "daily"},

            'extractor_bobble1001': {'main_path': "daily/extractors/extracted-metric-data/rtype=bobbleR1001",
                                     'size': 500 * MiB, 'marker': True, 'path_type': "daily"},

            'extractor_stay_focus1001': {'main_path': "daily/extractors/extracted-metric-data/rtype=stayFocusR1001",
                                     'size': 350 * MB, 'marker': True, 'path_type': "daily"},

            'extractor_bobble1005': {'main_path': "daily/extractors/extracted-metric-data/rtype=bobbleR1005",
                                     'size': 10 * MiB, 'marker': True, 'path_type': "daily"},

            'extractor_stay_focus1005': {'main_path': "daily/extractors/extracted-metric-data/rtype=stayFocusR1005",
                                     'size': 9 * MB, 'marker': True, 'path_type': "daily"},

            'extractor_bobble1008': {'main_path': "daily/extractors/extracted-metric-data/rtype=bobbleR1008",
                                     'size': 1 * GiB, 'marker': True, 'path_type': "daily"},

            'extractor_stay_focus1008': {'main_path': "daily/extractors/extracted-metric-data/rtype=stayFocusR1008",
                                     'size': 85 * MB, 'marker': True, 'path_type': "daily"},

            'extractor_mfour1008': {'main_path': "daily/extractors/extracted-metric-data/rtype=mfourR1008",
                                    'size': 18 * MiB, 'marker': True, 'path_type': "daily"},

            'bobble_installed_apps': {'main_path': 'raw-s2s/bobble-installed-apps', 'size': 120 * GiB,
                                      'marker': False, 'path_type': "daily"},

            'bobble_apps_sessions': {'main_path': 'raw-s2s/bobble-apps-sessions', 'size': 120 * GiB,
                                     'marker': False, 'path_type': "daily"},

            'source_apps': {'main_path': 'daily/source-apps', 'size': 1 * KB,
                                     'marker': True, 'path_type': "daily"},

            'extractor_ga_daily': {'main_path': 'raw-s2s/extractor_ga_daily', 'size': 10 * MiB,
                                   'marker': True, 'path_type': "monthly"},

            'extractor_ga_monthly': {'main_path': 'raw-s2s/extractor_ga_monthly', 'size': 450 * KiB,
                                     'marker': True, 'path_type': "monthly"},

            'extractor_kwh_daily': {'main_path': 'raw-s2s/extractor_kwh_daily', 'size': 3 * MiB,
                                    'marker': True, 'path_type': "monthly"},

            'extractor_kwh_monthly': {'main_path': 'raw-s2s/extractor_kwh_monthly', 'size': 250 * KiB,
                                      'marker': True, 'path_type': "monthly"},

            'extractor_kochava_daily_blacklist': {'main_path': 'raw-s2s/kochava_report/extractor_kochava_daily_blacklist', 'size': 650 * KiB,
                                        'marker': True, 'path_type': "monthly"},

            'extractor_kochava_daily_whitelist': {'main_path': 'raw-s2s/kochava_report/extractor_kochava_daily_whitelist', 'size': 650 * KiB,
                                                  'marker': True, 'path_type': "monthly"},

            'ptft_dau_factors': {'main_path': 'static/ptft/factors/dau', 'size': 380 * MiB,
                                        'marker': True, 'path_type': "base_path"},

            'ptft_installs_factors': {'main_path': 'static/ptft/factors/installs', 'size': 420 * MiB,
                                 'marker': True, 'path_type': "base_path"},

            'ptft_new_users_factors': {'main_path': 'static/ptft/factors/new_users', 'size': 450 * MiB,
                                 'marker': True, 'path_type': "base_path"},

            'ptft_reach_factors': {'main_path': 'static/ptft/factors/reach', 'size': 800 * MiB,
                                 'marker': True, 'path_type': "base_path"},

            'ptft_ww_store_download_factors_new_users_share': {'main_path': 'static/ptft/factors/ww_store_download/new_users_share', 'size': 350 * MiB,
                                 'marker': True, 'path_type': "base_path"},

            'ptft_ww_store_download_factors_installs_share': {'main_path': 'static/ptft/factors/ww_store_download/installs_share', 'size': 350 * MiB,
                                 'marker': True, 'path_type': "base_path"},

            'country_codes': {'main_path': 'country_codes', 'size': 1 * KB,
                                    'marker': True, 'path_type': "daily"},


            'extractor_kochava_daily': {'main_path': 'raw-s2s/extractor_kochava_daily', 'size': 650 * KiB,
                                        'marker': True, 'path_type': "monthly"},

            'mfour_apps_sessions': {'main_path': 'raw-s2s/mfour-apps-sessions', 'size': 80 * MiB,
                                    'marker': False, 'path_type': "daily"},

            'embee_app_session': {'main_path': "raw-stats-embee/app_session", 'size': 438 * MB,
                                  'marker': True, 'path_type': "daily"},

            'embee_device_info': {'main_path': "raw-stats-embee/device_info", 'size': 110 * MiB,
                                  'marker': True, 'path_type': "daily"},

            'embee_demographics': {'main_path': "raw-stats-embee/demographics", 'size': 6 * MiB,
                                   'marker': True, 'path_type': "daily"},

            'embee_naive_estimation': {'main_path': "raw-stats-embee/naive_estimation", 'size': 0 * MiB,
                                       'marker': True, 'path_type': "daily"},

            'embee_joined_data_output': {'main_path': "stats-mobile/parquet/rtype=R1111", 'size': 2.8 * GiB,
                                         'marker': True, 'path_type': "daily"},

            'stay_focus_installed_apps': {'main_path': 'raw-s2s/stay_focus-installed-apps', 'size': 0.48 * GiB,
                                      'marker': False, 'path_type': "daily"},

            'stay_focus_apps_sessions': {'main_path': 'raw-s2s/stay_focus-apps-sessions', 'size': 2.2 * GiB,
                                          'marker': False, 'path_type': "daily"},
            ###

            'grouping_1001_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1001", 'size': 20 * GiB,
                                             'marker': True,
                                             'path_type': "daily"},

            'grouping_1001_report_parquet_upsolver': {'main_path': "stats-mobile/parquet_adjusted/rtype=R1001", 'size': 7 * GiB,
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

            'grouping_1005_report_parquet_upsolver': {'main_path': "stats-mobile/parquet_adjusted/rtype=R1005", 'size': 70 * MB,
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

            'grouping_1111_report_parquet': {'main_path': "stats-mobile/parquet/rtype=R1111", 'size': 2.8 * GiB,
                                             'marker': True,
                                             'path_type': "daily"},

            'mdm_2001_report': {'main_path': "stats-ios/orc/rtype=2001", 'size': 10 * MB,
                                             'marker': True,
                                             'path_type': "daily"},

            'mdm_snapshot': {'main_path': "%s/mdm/type=%s" % (self.ti.mode, self.ti.mode_type),
                             'size': 1 * MB,
                             'marker': True,
                             'path_type': "monthly"},

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
                'size': 150 * MB, 'marker': True,
                'path_type': "daily"},

            # Snapshot/WindowF
            'app_scores': {
                'main_path': "%(mode)s/app-scores/type=%(mode_type)s" % {'mode': self.ti.mode, 'mode_type': self.ti.mode_type},
                'size': 97 * MiB, 'marker': False,  # Size close for both window ,and snapshot
                'path_type': self.ti.mode_type},

            'app_scores_with_info': {
                'main_path': "%(mode)s/app-scores-with-info/type=%(mode_type)s" % {'mode': self.ti.mode, 'mode_type': self.ti.mode_type},
                'size': 140 * MiB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': self.ti.mode_type},

            'category_ranks': {
                'main_path': "%(mode)s/category-ranks/type=%(mode_type)s" % {'mode': self.ti.mode, 'mode_type': self.ti.mode_type},
                'size': 100 * MiB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': self.ti.mode_type},

            'trending_apps': {
                'main_path': "%(mode)s/trending-apps/type=%(mode_type)s" % {'mode': self.ti.mode, 'mode_type': self.ti.mode_type},
                'size': 26 * MiB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': self.ti.mode_type},

            'trending_apps_parquet': {
                'main_path': "%(mode)s/trending-apps-parquet/type=%(mode_type)s" % {'mode': self.ti.mode, 'mode_type': self.ti.mode_type},
                'size': 35 * MiB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': self.ti.mode_type},

            'category_ranks_parquet': {
                'main_path': "%(mode)s/category-ranks-parquet/type=%(mode_type)s" % {'mode': self.ti.mode, 'mode_type': self.ti.mode_type},
                'size': 150 * MiB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': self.ti.mode_type},

            'store_category_ranks': {
                'main_path': "%(mode)s/store_category_ranks/type=%(mode_type)s" % {'mode': self.ti.mode, 'mode_type': self.ti.mode_type},
                'size': 100 * MB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': self.ti.mode_type},

            'usage_climbing_apps': {
                'main_path': "%(mode)s/usage-climbing-apps/type=%(mode_type)s" % {'mode': self.ti.mode, 'mode_type': self.ti.mode_type},
                'size': 2 * MiB,
                'marker': True,
                'path_type': self.ti.mode_type},

            'usage_slipping_apps': {
                'main_path': "%(mode)s/usage-slipping-apps/type=%(mode_type)s" % {'mode': self.ti.mode, 'mode_type': self.ti.mode_type},
                'size': 2 * MiB,
                'marker': True,
                'path_type': self.ti.mode_type},

            'usage_ranking_apps': {
                'main_path': "%(mode)s/usage-ranking-apps-parquet/type=%(mode_type)s" % {'mode': self.ti.mode, 'mode_type': self.ti.mode_type},
                'size': 1 * MiB,
                'marker': True,
                'path_type': self.ti.mode_type},


            'app_panel': {
                'main_path': "%s/estimate/app-panel/type=%s" % (self.ti.mode, self.ti.mode_type),
                'size': 1 * KiB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': "monthly"},

            'country_panel': {
                'main_path': "%s/estimate/country-panel/type=%s" % (self.ti.mode, self.ti.mode_type),
                'size': 1 * KiB, 'marker': True,  # Size close for both window ,and snapshot
                'path_type': "monthly"},

            'app_affinity': {'main_path': "%s/estimate/app-affinity-country/type=%s" % (self.ti.mode, self.ti.mode_type),
                             'size': 1 * KiB, 'marker': True,
                             'path_type': "monthly"},

            'app_affinity_with_ww': {'main_path': "%s/estimate/app-affinity-ww/type=%s" % (self.ti.mode, self.ti.mode_type),
                             'size': 1 * KiB, 'marker': True,
                             'path_type': "monthly"},

            'app_affinity_pairs': {'main_path': "daily/aggregations/aggkey=AppPairCountryKey",
                             'size': 1 * GiB, 'marker': True,
                             'path_type': "daily"},

            'app_affinity_pairs_agg': {'main_path': "%s/estimate/app_affinity_pairs_agg/type=%s" % (self.ti.mode, self.ti.mode_type),
                             'size': 1 * KiB, 'marker': True,
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
                                   'size': 850 * MiB, 'marker': True,
                                   'path_type': "daily"},

            'ww_downloads_to_scrape': {'main_path': "scraping/ww-downloads-to-scrape",
                         'size': 5.0 * MB, 'marker': True,
                         'path_type': "daily"},

            # store-analysis
            'store_ranks': {'main_path': "mobile/store-ranks/type=last-1",
                            'size': 11 * MiB, 'marker': True,
                            'path_type': "daily"},


            # # Reviews

            # # # Data Paths

            'google_play_raw_reviews': {'main_path': "google-play/reviews/raw",
                                       'size': 1 * MiB, 'marker': True,
                                       'path_type': "daily"},

            'ios_app_store_raw_reviews': {'main_path': "iOS-app-store/reviews/raw",
                                        'size': 1 * MiB, 'marker': True,
                                        'path_type': "daily"},

            'google_play_reviews_sentiment': {'main_path': "google-play/reviews/sentiment",
                                          'size': 1 * MiB, 'marker': True,
                                          'path_type': "daily"},

            'ios_app_store_reviews_sentiment': {'main_path': "iOS-app-store/reviews/sentiment",
                                          'size': 1 * MiB, 'marker': True,
                                          'path_type': "daily"},

            'version_db_dump': {'main_path': "all-store-version-db-dump",
                                                'size': 1 * MiB, 'marker': True,
                                                'path_type': "daily"},

            'google_play_preprocessed_reviews': {'main_path': "google-play/reviews/preprocessed",
                                        'size': 0 * MiB, 'marker': True,
                                        'path_type': "daily"},

            'ios_app_store_preprocessed_reviews': {'main_path': "iOS-app-store/reviews/preprocessed",
                                          'size': 1 * MiB, 'marker': True,
                                          'path_type': "daily"},

            'google_play_analyzed_reviews': {'main_path': "google-play/reviews/analyzed",
                                                 'size': 0 * MiB, 'marker': True,
                                                 'path_type': "daily"},

            'ios_app_store_analyzed_reviews': {'main_path': "iOS-app-store/reviews/analyzed",
                                                   'size': 1 * MiB, 'marker': True,
                                                   'path_type': "daily"},

            'google_play_nlp_transformed_reviews': {'main_path': "google-play/reviews/topic_analysis/nlp_transformed",
                                    'size': 0 * MiB, 'marker': True,
                                    'path_type': "daily"},

            'ios_app_store_nlp_transformed_reviews': {'main_path': "iOS-app-store/reviews/topic_analysis/nlp_transformed",
                                      'size': 1 * MiB, 'marker': True,
                                      'path_type': "daily"},

            'google_play_inferred_reviews': {'main_path': "google-play/reviews/topic_analysis/inferred",
                                              'size': 0 * MiB, 'marker': False,
                                              'path_type': "daily"},

            'ios_app_store_inferred_reviews': {'main_path': "iOS-app-store/reviews/topic_analysis/inferred",
                                                'size': 0 * MiB, 'marker': False,
                                                'path_type': "daily"},

            'google_play_reviews_topic_analysis': {'main_path': "google-play/reviews/topic_analysis/aggregated",
                                              'size': 0 * MiB, 'marker': False,
                                              'path_type': "daily"},

            'ios_app_store_reviews_topic_analysis': {'main_path': "iOS-app-store/reviews/topic_analysis/aggregated",
                                                'size': 0 * MiB, 'marker': False,
                                                'path_type': "daily"},

            'google_play_reviews_per_app': {'main_path': "google-play/reviews/per_app",
                                               'size': 1 * MiB, 'marker': True,
                                               'path_type': "daily"},

            'ios_app_store_reviews_per_app': {'main_path': "iOS-app-store/reviews/per_app",
                                                 'size': 1 * MiB, 'marker': True,
                                                 'path_type': "daily"},

            # # # Model Paths

            'google_play_reviews_nlp_pipeline': {'main_path': "google-play/reviews/topic_analysis/nlp_pipeline",
                                      'size': 0, 'marker': False,
                                      'path_type': "base_path"},

            'ios_app_store_reviews_nlp_pipeline': {'main_path': "iOS-app-store/reviews/topic_analysis/nlp_pipeline",
                                              'size': 0, 'marker': False,
                                              'path_type': "base_path"},

            'google_play_reviews_inference_model': {'main_path': "google-play/reviews/topic_analysis/inference_model",
                                              'size': 0, 'marker': False,
                                              'path_type': "base_path"},

            'ios_app_store_reviews_inference_model': {'main_path': "iOS-app-store/reviews/topic_analysis/inference_model",
                                                'size': 0, 'marker': False,
                                                'path_type': "base_path"},
            # # Version DB

            'google_play_version_db': {'main_path': "google-play/app_version_db",
                         'size': 100 * MiB, 'marker': False,
                         'path_type': "base_path"},

            'ios_app_store_version_db': {'main_path': "iOS-app-store/app_version_db",
                                       'size': 100 * MiB, 'marker': False,
                                       'path_type': "base_path"},

            # # Timeline DB

            'google_play_timeline_db': {'main_path': "google-play/app_timeline_db",
                                        'size': 100 * MiB, 'marker': False,
                                        'path_type': "base_path"},

            'ios_app_store_timeline_db': {'main_path': "iOS-app-store/app_timeline_db",
                                          'size': 100 * MiB, 'marker': False,
                                          'path_type': "base_path"},

            # # GA4 extractor

            'app_store_ga4_raw': {'main_path': "general/ga4_extractor/raw_data",
                                  'size': 26 * MB, 'marker': True,
                                  'path_type': "daily"},

            'app_store_ga4_clean_table': {'main_path': "general/ga4_extractor/clean_table",
                                          'size': 25 * MB, 'marker': True,
                                          'path_type': "daily"},

            'app_store_ga4_recognize_full': {'main_path': "general/ga4_extractor/recognize/full_table",
                                             'size': 7 * MB, 'marker': True,
                                             'path_type': "daily"},

            'app_store_ga4_recognize_missings': {'main_path': "general/ga4_extractor/recognize/missings",
                                                 'size': 1 * KB, 'marker': True,
                                                 'path_type': "daily"},

            'app_store_ga4_recognize_db': {'main_path': "general/ga4_extractor/recognize/db_missings",
                                           'size': 25 * MB, 'marker': False,
                                           'path_type': "base_path"},

            'app_store_ga4_filling_table': {'main_path': "general/ga4_extractor/prophet/full_table",
                                            'size': 1 * KB, 'marker': True,
                                            'path_type': "daily"},

            'ga4_processed_ls': {'main_path': "general/ga4_extractor/processed_ls",
                                 'size': 12 * MB, 'marker': True,
                                 'path_type': "daily"},

            # # Ratings

            'google_play_ratings': {'main_path': 'google-play/ratings',
                                      'size': 200 * MB, 'marker': True,
                                      'path_type': 'daily'},

            'ios_app_store_ratings': {'main_path': 'iOS-app-store/ratings',
                                      'size': 10 * MB, 'marker': True,
                                      'path_type': 'daily'},


            # Static paths
            'countries_full_names': {'main_path': "resources/country-codes-dict",
                                       'size': 1 * KiB, 'marker': False,
                                       'path_type': "base_path"},

            'z_norm_dist': {'main_path': "static/norm_dist",
                                     'size': 1 * KiB, 'marker': True,
                                     'path_type': "base_path"},
            'static_matching_predict': {'main_path': "static/old-apps-matching",
                                    'size': 5 * KiB, 'marker': True,
                                    'path_type': "base_path"},

            'new_static_matching_predict': {'main_path': "static/new-apps-matching",
                                        'size': 5 * KiB, 'marker': True,
                                        'path_type': "base_path"},

            'static_affinity':{'main_path': "static/affinity",
                                       'size': 14 * GB, 'marker': True,
                                       'path_type': "base_path"},

            'static_mobidays_usage_fix': {'main_path': "static/mobidays-mitigation/usage/agg",
                                      'size': 1 * MB, 'marker': True,
                                          'path_type': "base_path"},

            # retention
            'aggregated_retention':{'main_path': "retention/aggregated-retention",
                                    'size': 50 * MiB, 'marker': True,
                                    'path_type': "daily"},

            'aggregated_retention_before_mobidays':{'main_path': "retention/aggregated-retention-before-mobidays",
                                    'size': 50 * MiB, 'marker': True,
                                    'path_type': "daily"},

            'static_mobidays_retention_fix': {'main_path': "static/mobidays-mitigation/retention/agg",
                                              'size': 1 * MB, 'marker': True,
                                              'path_type': "base_path"},

            'preprocess_retention': {'main_path': "retention/preprocess-retention",
                                     'size': 1.5 * MB, 'marker': True,
                                     'path_type': "daily"},

            'calc_retention': {'main_path': "retention/calc-retention",
                               'size': 100 * KB, 'marker': True,
                               'path_type': "daily"},

            'ww_smoothing_retention': {'main_path': "retention/ww-smoothing-retention",
                                               'size': 18 * MB, 'marker': True,
                                               'path_type': "daily"},

            'category_smoothing_retention': {'main_path': "retention/category-smoothing-retention",
                                       'size': 800, 'marker': True,
                                       'path_type': "daily"},

            'top_app_smoothing_retention': {'main_path': "retention/top-app-smoothing-retention",
                                       'size': 40 * KB, 'marker': True,
                                       'path_type': "daily"},

            'final_prior_smoothing_retention': {'main_path': "retention/final-prior-smoothing-retention",
                                               'size': 100 * KB, 'marker': True,
                                               'path_type': "daily"},

            'estimated_retention': {'main_path': "retention/estimated-retention",
                                    'size': 100 * KB, 'marker': True,
                                    'path_type': "daily"},

            'categories_estimated_retention': {'main_path': "retention/categories-estimated-retention",
                                    'size': 100 * KB, 'marker': True,
                                    'path_type': "daily"},

            #OPEN RATE
            'source_raw_estimation_open_rate': {'main_path': "open-rate/source_raw_estimation_open_rate",
                                   'size': 1.4 * MiB,
                                   'marker': True, 'path_type': "daily"},

            'merge_sources_open_rate': {'main_path': "open-rate/merge_sources_open_rate",
                              'size': 100 * KB,
                              'marker': True, 'path_type': "daily"},

            'priors_open_rate': {'main_path': "open-rate/priors_open_rate",
                                            'size': 100 * KB,
                                            'marker': True, 'path_type': "daily"},

            'combined_open_rate': {'main_path': "open-rate/combined_open_rate",
                              'size': 1 * MiB,
                              'marker': True, 'path_type': "daily"},

            'final_ww_open_rate': {'main_path': "open-rate/final_ww_open_rate",
                              'size': 1 * MiB,
                              'marker': True, 'path_type': "daily"},

            # install_retention
            'aggregated_install_retention_1001':{'main_path': "installretention/aggregated-installretention-1001",
                                                 'size': 50 * MiB, 'marker': True,
                                                 'path_type': "daily"},
            'aggregated_install_retention_1005':{'main_path': "installretention/aggregated-installretention-1005",
                                                 'size': 50 * MiB, 'marker': True,
                                                 'path_type': "daily"},

            'preprocess_install_retention': {'main_path': "installretention/preprocess-installretention",
                                     'size': 1.5 * MB, 'marker': True,
                                     'path_type': "daily"},

            'calc_install_retention': {'main_path': "installretention/calc-installretention",
                               'size': 100 * KB, 'marker': True,
                               'path_type': "daily"},

            'ww_smoothing_install_retention': {'main_path': "installretention/ww-smoothing-installretention",
                                       'size': 18 * MB, 'marker': True,
                                       'path_type': "daily"},

            'category_smoothing_install_retention': {'main_path': "installretention/category-smoothing-installretention",
                                             'size': 800, 'marker': True,
                                             'path_type': "daily"},

            'top_app_smoothing_install_retention': {'main_path': "installretention/top-app-smoothing-installretention",
                                            'size': 40 * KB, 'marker': True,
                                            'path_type': "daily"},

            'final_prior_smoothing_install_retention': {'main_path': "installretention/final-prior-smoothing-installretention",
                                                'size': 100 * KB, 'marker': True,
                                                'path_type': "daily"},

            'estimated_install_retention': {'main_path': "installretention/estimated-installretention",
                                    'size': 100 * KB, 'marker': True,
                                    'path_type': "daily"},

            'categories_estimated_install_retention': {'main_path': "installretention/categories-estimated-installretention",
                                               'size': 100 * KB, 'marker': True,
                                               'path_type': "daily"},


            ### Usage Patterns

            'country_workweek_days': {'main_path': "static/country_workweek_days",
                                            'size': 1 , 'marker': True, 'path_type': "base_path"},

            'usage_patterns_session_list': {'main_path': "usage_patterns/{mode}/raw_sessions",
                                   'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_source_raw_estimation': {'main_path': "usage_patterns/{mode}/sources_raw_estimation",
                                                         'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_raw_estimation': {'main_path': "usage_patterns/{mode}/raw_estimation",
                                            'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_estimation_with_ww': {'main_path': "usage_patterns/{mode}/estimation_with_ww",
                                                          'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_beta_binomial_estimation': {'main_path': "usage_patterns/{mode}/merged_weeks_estimation",
                                                  'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_weeks_merged': {'main_path': "usage_patterns/{mode}/merged_weeks_estimation",
                                                        'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_category_estimation': {'main_path': "usage_patterns/{mode}/category_estimation",
                                                              'size': 1 * KB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_prior_estimation': {'main_path': "usage_patterns/{mode}/priors_estimation",
                                                            'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_final_estimation': {'main_path': "usage_patterns/{mode}/final_estimation",
                                           'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_fused_prior_estimation': {'main_path': "usage_patterns/{mode}/fused_prior_estimation",
                                                     'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_stability_estimation': {'main_path': "usage_patterns/{mode}/stability_monitoring/stability_estimation",
                                                'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_clean_for_monitoring': {'main_path': "usage_patterns/{mode}/stability_monitoring/clean_metric",
                                                    'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'usage_patterns_final_estimation_snapshot': {'main_path': "usage_patterns/snapshot/{mode}/final_estimation",
                                                                  'size': 1 * MB, 'marker': True, 'path_type': "monthly"},

            'usage_patterns_category_final_estimation_snapshot': {'main_path': "usage_patterns/snapshot/{mode}/category_final_estimation",
                                                'size': 1 * KB, 'marker': True, 'path_type': "monthly"},
            #demographics
            'raw_stats_mobile': {'main_path': "stats-mobile/raw", 'size': 15 * GiB, 'marker': True, 'path_type': "daily"},

            'user_info': {'main_path': "mobile-analytics/demographics/user_info",
                          'size': 1 * MB, 'marker': True, 'path_type': "daily"},

            'age_model': {'main_path': "mobile-analytics/demographics/models/android_age_model_multinomial",
                          'size': 28 * MB, 'marker': True, 'path_type': "base_path"},

            'gender_model': {'main_path': "mobile-analytics/demographics/models/android_gender_model_no_pca",
                          'size': 425 * KB, 'marker': True, 'path_type': "base_path"},

            'apps_all_age': {'main_path': "mobile-analytics/demographics/demographic_distribution/age/apps_all_age",
                          'size': 20 * MB, 'marker': False, 'path_type': "daily"},

            'apps_country_age': {'main_path': "mobile-analytics/demographics/demographic_distribution/age/apps_country_age",
                             'size': 55 * MB, 'marker': False, 'path_type': "daily"},

            'apps_all_gender': {'main_path': "mobile-analytics/demographics/demographic_distribution/gender/apps_all_gender",
                             'size': 20 * MB, 'marker': False, 'path_type': "daily"},

            'apps_country_gender': {'main_path': "mobile-analytics/demographics/demographic_distribution/gender/apps_country_gender",
                                 'size': 60 * MB, 'marker': False, 'path_type': "daily"},

            'apps_all_age_aggregate': {'main_path': "mobile-analytics/snapshot/app-demographics/age/apps_all_age",
                             'size': 60 * MB, 'marker': False, 'path_type': "monthly"},

            'apps_country_age_aggregate': {'main_path': "mobile-analytics/snapshot/app-demographics/age/apps_country_age",
                                 'size': 200 * MB, 'marker': False, 'path_type': "monthly"},

            'apps_all_gender_aggregate': {'main_path': "mobile-analytics/snapshot/app-demographics/gender/apps_all_gender",
                                'size': 50 * MB, 'marker': False, 'path_type': "monthly"},

            'apps_country_gender_aggregate': {'main_path': "mobile-analytics/snapshot/app-demographics/gender/apps_country_gender",
                                    'size': 180 * MB, 'marker': False, 'path_type': "monthly"},

            #ios_mau
            'new_apps_matching_static': {'main_path': "ios-analytics/snapshot/mau_dp/ios_mau_static/new_apps_matching_static",
                                              'size': 180 * MB, 'marker': True, 'path_type': "monthly"},

            'new_ios_conv': {'main_path': "ios-analytics/snapshot/mau_dp/ios_mau_static/new_ios_conv",
                                         'size': 180 * MB, 'marker': True, 'path_type': "monthly"},

            'old_coverage_static': {'main_path': "ios-analytics/snapshot/mau_dp/ios_mau_static/old_coverage_static",
                                         'size': 180 * MB, 'marker': True, 'path_type': "monthly"},

            'ios_features_10': {'main_path': "%s/mau_dp/features10" % (self.ti.mode),
                               'size': 1 * MB, 'marker': True, 'path_type': "monthly"},

            'ios_features_11': {'main_path': "%s/mau_dp/features11" % (self.ti.mode),
                               'size': 1 * MB, 'marker': True, 'path_type': "monthly"},

            'get_downloads_raw_app_country_country_source_agg': {'main_path': "daily/downloads/aggregations/raw/aggKey=AppCountryCountrySourceKey",
                                                                 'size': 4 * GB,
                                                                 'marker': True, 'path_type': "daily"},

            'get_static_mobidays_fix_downloads_app_country_country_source_agg': {'main_path': "static/mobidays-mitigation/downloads/aggregations/new_calc/aggKey=AppCountryCountrySourceKey",
                                                                                 'size': 1 ,
                                                                                 'marker': True, 'path_type': "daily"},

            'get_dau_raw_app_country_source_agg': {'main_path': "daily/dau/aggregations/raw/aggKey=AppCountrySourceKey",
                                                   'size': 1 * MiB,
                                                   'marker': True, 'path_type': "daily"},

            'get_static_mobidays_fix_app_country_source_agg': {'main_path': "static/mobidays-mitigation/dau/aggregations/new_calc/aggKey=AppCountrySourceKey",
                                                               'size': 1 ,
                                                               'marker': True, 'path_type': "daily"},

            'get_dau_raw_country_source_agg': {'main_path': "daily/dau/aggregations/raw/aggKey=CountrySourceKey",
                                               'size': 10 * KB,
                                               'marker': True, 'path_type': "daily"},

            'get_static_mobidays_fix_country_source_agg': {'main_path': "static/mobidays-mitigation/dau/aggregations/new_calc/aggKey=CountrySourceKey",
                                                           'size': 1 ,
                                                           'marker': True, 'path_type': "daily"},

        }
        self.paths_dates_suffix = {
            'dau_factors': [
                date(2021, 6, 30),
                date(2021, 7, 18)
            ],
            'installs_factors': [
                date(2021, 6, 30),
                date(2022, 1, 19)
            ],
            'new_users_factors': [
                date(2021, 6, 30),
                date(2022, 1, 19)
            ],
            'reach_factors': [
                date(2021, 6, 30),
                date(2022, 1, 19)
            ],

            'ww_store_download_factors': [
                date(2021, 6, 30)
            ],
        }

    def __get_base_dir(self, in_or_out, path_prefix):
        base_dir = self.ti.base_dir if in_or_out == "in" else self.ti.calc_dir
        return base_dir if not path_prefix else path_join(path_prefix, base_dir)

    def __get_atlanta_base(self, in_or_out, path_prefix):
        base_dir = self.__get_base_dir(in_or_out, path_prefix)
        return path_join(base_dir, "atlanta")

    def __get_android_apps_analytics_base(self, in_or_out, path_prefix):
        base_dir = self.__get_base_dir(in_or_out, path_prefix)
        return path_join(base_dir, "android-apps-analytics")

    def __get_ios_analytics_base(self, in_or_out, path_prefix):
        base_dir = self.__get_base_dir(in_or_out, path_prefix)
        return path_join(base_dir, "ios-analytics")

    def __get_os_base(self, in_or_out, path_prefix, os):
        if os != 'android' and os !='ios':
            raise ValueError("Operation System could be only 'android' or 'ios'")
        return self.__get_android_apps_analytics_base(in_or_out, path_prefix) if os == 'android' else self.__get_ios_analytics_base(in_or_out, path_prefix)

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

    def __get_max_date_below_exec_date(self, date_suffix_key):
        dates = self.paths_dates_suffix[date_suffix_key]
        max_date = None
        for d in dates:
            if d < self.ti.date and (max_date is None or d > max_date):
                max_date = d
        if max_date is None:
            raise Exception(("AppsPathSolver - Couldn't find a date before execution date!"))
        return max_date


    #Atlanta Getters

    def get_aa_android_engagement(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_atlanta_base(in_or_out, path_prefix),
                                             self.apps_paths['aa_android_engagement'], path_suffix, in_or_out)

    def get_aa_ios_engagement(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_atlanta_base(in_or_out, path_prefix),
                                             self.apps_paths['aa_ios_engagement'], path_suffix, in_or_out)

    def get_aa_android_d30_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_atlanta_base(in_or_out, path_prefix),
                                             self.apps_paths['aa_android_d30_retention'], path_suffix, in_or_out)

    def get_aa_ios_d30_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_atlanta_base(in_or_out, path_prefix),
                                             self.apps_paths['aa_ios_d30_retention'], path_suffix, in_or_out)

    def get_aa_android_cross_affinity(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_atlanta_base(in_or_out, path_prefix),
                                             self.apps_paths['aa_android_cross_affinity'], path_suffix, in_or_out)

    def get_aa_ios_cross_affinity(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_atlanta_base(in_or_out, path_prefix),
                                             self.apps_paths['aa_ios_cross_affinity'], path_suffix, in_or_out)


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

    def get_extractor_stay_focus1001(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_stay_focus1001'], path_suffix, in_or_out)

    def get_extractor_bobble1005(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_bobble1005'], path_suffix, in_or_out)

    def get_extractor_stay_focus1005(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_stay_focus1005'], path_suffix, in_or_out)

    def get_extractor_bobble1008(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_bobble1008'], path_suffix, in_or_out)

    def get_extractor_stay_focus1008(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_stay_focus1008'], path_suffix, in_or_out)

    def get_extractor_mfour1008(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_mfour1008'], path_suffix, in_or_out)

    def get_source_apps(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['source_apps'], path_suffix, in_or_out)

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

    def get_extractor_kochava_daily(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_kochava_daily'], path_suffix, in_or_out)

    def get_extractor_kochava_daily_blacklist(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_kochava_daily_blacklist'], path_suffix, in_or_out)

    def get_extractor_kochava_daily_whitelist(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['extractor_kochava_daily_whitelist'], path_suffix, in_or_out)

    def get_ptft_dau_factors(self, in_or_out, path_prefix=None, path_suffix=None):
        if in_or_out == 'in' and path_suffix is None:
            path_suffix = self.ti.year_month_day(self.__get_max_date_below_exec_date('dau_factors'))
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ptft_dau_factors'], path_suffix, in_or_out)

    def get_ptft_installs_factors(self, in_or_out, path_prefix=None, path_suffix=None):
        if in_or_out == 'in' and path_suffix is None:
            path_suffix = self.ti.year_month_day(self.__get_max_date_below_exec_date('installs_factors'))
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ptft_installs_factors'], path_suffix, in_or_out)

    def get_ptft_new_users_factors(self, in_or_out, path_prefix=None, path_suffix=None):
        if in_or_out == 'in' and path_suffix is None:
            path_suffix = self.ti.year_month_day(self.__get_max_date_below_exec_date('new_users_factors'))
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ptft_new_users_factors'], path_suffix, in_or_out)

    def get_ptft_reach_factors(self, in_or_out, path_prefix=None, path_suffix=None):
        if in_or_out == 'in' and path_suffix is None:
            path_suffix = self.ti.year_month_day(self.__get_max_date_below_exec_date('reach_factors'))
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                            self.apps_paths['ptft_reach_factors'], path_suffix, in_or_out)


    def get_ptft_ww_store_download_factors_installs_share(self, in_or_out, path_prefix=None, path_suffix=None):
        if in_or_out == 'in' and path_suffix is None:
            path_suffix = self.ti.year_month_day(self.__get_max_date_below_exec_date('ww_store_download_factors'))
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                            self.apps_paths['ptft_ww_store_download_factors_installs_share'], path_suffix, in_or_out)


    def get_ptft_ww_store_download_factors_new_users_share(self, in_or_out, path_prefix=None, path_suffix=None):
        if in_or_out == 'in' and path_suffix is None:
            path_suffix = self.ti.year_month_day(self.__get_max_date_below_exec_date('ww_store_download_factors'))
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                            self.apps_paths['ptft_ww_store_download_factors_new_users_share'], path_suffix, in_or_out)

    def get_country_codes(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['country_codes'], path_suffix, in_or_out)


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

    def get_mdm_2001_report(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['mdm_2001_report'], path_suffix, in_or_out)

    def get_mdm_snapshot(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_ios_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mdm_snapshot'], path_suffix, in_or_out)

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

    def get_downloads_intermediate_sfa(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['downloads_intermediate_sources_for_analyze'], path_suffix, in_or_out)

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
    def get_matching_hash_image_features_ios(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-hash-image-features-ios'], path_suffix, in_or_out)
    def get_matching_hash_image_features_android(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-hash-image-features-android'], path_suffix, in_or_out)
    def get_matching_language_features_ios(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-language-features-ios'], path_suffix, in_or_out)
    def get_matching_language_features_android(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-language-features-android'], path_suffix, in_or_out)
    def get_matching_training_data(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-training-data'], path_suffix, in_or_out)
    def get_matching_train_data(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-train-data'], path_suffix, in_or_out)
    def get_matching_test_data(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-test-data'], path_suffix, in_or_out)
    def get_matching_classification_data(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-classification-data'], path_suffix, in_or_out)
    def get_matching_model(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-model'], path_suffix, in_or_out)
    def get_matching_raw_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-raw-predict'], path_suffix, in_or_out)
    def get_matching_predict_sanitized(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                         self.apps_paths['matching-predict-sanitized'], path_suffix, in_or_out)
    def get_matching_predict_consistency(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-predict-consistency'], path_suffix, in_or_out)
    def get_matching_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-predict'], path_suffix, in_or_out)
    def get_matching_stats_candidates_coverage(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-stats-candidates-coverage'], path_suffix, in_or_out)
    def get_matching_stats_model(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-stats-model'], path_suffix, in_or_out)
    def get_matching_stats_prediction_coverage(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-stats-prediction-coverage'], path_suffix, in_or_out)
    def get_matching_stats_prediction_accuracy(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-stats-prediction-accuracy'], path_suffix, in_or_out)
    def get_matching_stats_prediction_vs_manual_accuracy(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-stats-prediction-manual-accuracy'], path_suffix, in_or_out)
    def get_matching_stats_prediction_vs_top_ls_accuracy(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['matching-stats-prediction-top-ls-accuracy'], path_suffix, in_or_out)
    def get_matching_rds(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                            self.apps_paths['matching-rds'], path_suffix, in_or_out)

    #open_rate
    def get_source_raw_estimation_open_rate(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['source_raw_estimation_open_rate'], path_suffix, in_or_out)
    def get_merge_sources_open_rate(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['merge_sources_open_rate'], path_suffix, in_or_out)
    def get_priors_open_rate(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['priors_open_rate'], path_suffix, in_or_out)
    def get_combined_open_rate(self, in_or_out, path_prefix=None, path_suffix=None):
            return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                                 self.apps_paths['combined_open_rate'], path_suffix, in_or_out)
    def get_final_ww_open_rate(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['final_ww_open_rate'], path_suffix, in_or_out)
    # apps monitoring
    def get_monitoring_dau_window(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-dau-window'], path_suffix, in_or_out)
    def get_monitoring_open_rate_window(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-open-rate-window'], path_suffix, in_or_out)
    def get_monitoring_downloads_window(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-downloads-window'], path_suffix, in_or_out)
    def get_monitoring_reach_window(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                         self.apps_paths['monitoring-reach-window'], path_suffix, in_or_out)
    def get_monitoring_unique_installs_window(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-unique-installs-window'], path_suffix, in_or_out)
    def get_monitoring_sessions_window(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-sessions-window'], path_suffix, in_or_out)
    def get_monitoring_usagetime_window(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-usagetime-window'], path_suffix, in_or_out)
    def get_monitoring_retention_window(self, in_or_out, path_prefix=None, path_suffix=None, td = 0):
        self.apps_paths['monitoring-retention-window']['main_path'] = "apps-monitoring/retention/window%(td)s" % {'td':td}
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-retention-window'], path_suffix, in_or_out)
    def get_monitoring_dau_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-dau-predict'], path_suffix, in_or_out)
    def get_monitoring_open_rate_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-open-rate-predict'], path_suffix, in_or_out)
    def get_monitoring_downloads_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-downloads-predict'], path_suffix, in_or_out)
    def get_monitoring_reach_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-reach-predict'], path_suffix, in_or_out)
    def get_monitoring_unique_installs_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                         self.apps_paths['monitoring-unique-installs-predict'], path_suffix, in_or_out)
    def get_monitoring_sessions_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-sessions-predict'], path_suffix, in_or_out)
    def get_monitoring_usagetime_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-usagetime-predict'], path_suffix, in_or_out)
    def get_monitoring_retention_predict(self, in_or_out, path_prefix=None, path_suffix=None, td = 0):
        self.apps_paths['monitoring-retention-predict']['main_path'] = "apps-monitoring/retention/predict%(td)s" % {'td':td}
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-retention-predict'], path_suffix, in_or_out)
    def get_monitoring_dau_anomal_zscores(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-dau-anomal-zscores'], path_suffix, in_or_out)
    def get_monitoring_open_rate_anomal_zscores(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-open-rate-anomal-zscores'], path_suffix, in_or_out)
    def get_monitoring_downloads_anomal_zscores(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-downloads-anomal-zscores'], path_suffix, in_or_out)
    def get_monitoring_reach_anomal_zscores(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-reach-anomal-zscores'], path_suffix, in_or_out)
    def get_monitoring_unique_installs_anomal_zscores(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-unique-installs-anomal-zscores'], path_suffix, in_or_out)
    def get_monitoring_retention_anomal_zscores(self, in_or_out, path_prefix=None, path_suffix=None, td = 0):
        self.apps_paths['monitoring-retention-anomal-zscores']['main_path'] = "apps-monitoring/retention/zScores%(td)s" % {'td':td}
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-retention-anomal-zscores'], path_suffix, in_or_out)
    def get_monitoring_retention_anomal_countries(self, in_or_out, path_prefix=None, path_suffix=None, td = 0):
        self.apps_paths['monitoring-retention-anomal-countries']['main_path'] = "apps-monitoring/retention/countries%(td)s" % {'td':td}
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-retention-anomal-countries'], path_suffix, in_or_out)
    def get_monitoring_sessions_anomal_zscores(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-sessions-anomal-zscores'], path_suffix, in_or_out)
    def get_monitoring_usagetime_anomal_zscores(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-usagetime-anomal-zscores'], path_suffix, in_or_out)
    def get_monitoring_mau_window(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-mau-window'], path_suffix, in_or_out)
    def get_monitoring_mau_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                         self.apps_paths['monitoring-mau-predict'], path_suffix, in_or_out)
    def get_monitoring_mau_anomal_zscores(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-mau-anomal-zscores'], path_suffix, in_or_out)
    def get_monitoring_mau_anomal_countries(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['monitoring-mau-anomal-countries'], path_suffix, in_or_out)

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

    def get_ww_store_download_for_ptft(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ww_store_download_for_ptft'], path_suffix, in_or_out)


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

    # usage
    def get_usage_agg_app_country_before_mobidays_fix(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_agg_app_country_before_mobidays_fix'], path_suffix, in_or_out)
    def get_usage_agg_app_country(self, in_or_out, path_prefix=None, path_suffix=None):
            return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                                 self.apps_paths['usage_agg_app_country_after_mobidays_fix'], path_suffix, in_or_out)
    def get_usage_prior(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_prior'], path_suffix, in_or_out)
    def get_usage_estimation(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_estimation'], path_suffix, in_or_out)
    def get_usage_estimation_ww(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_estimation_ww'], path_suffix, in_or_out)

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

    def get_mau_predict_with_ww(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_predict_with_ww'], path_suffix, in_or_out)

    def get_mau_android_factors(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_android_factors'], path_suffix, in_or_out)

    def get_mau_embee_est(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_embee_estimate'], path_suffix, in_or_out)

    def get_mau_weighted_embee_est(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_weighted_embee_est'], path_suffix, in_or_out)

    def get_mau_pre_est(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_pre_est'], path_suffix, in_or_out)

    # Replace old mau pdl path.
    def get_mau_dau_adjusted_estimation_rn(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['mau_dau_adjusted_estimation_rn'], path_suffix, in_or_out)
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

    def get_app_info(self, in_or_out, store, path_prefix=None):
        if store == GOOGLE_PLAY:
            return self.get_android_app_info(in_or_out, path_prefix)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_info(in_or_out, path_prefix)

    def get_raw_android_app_info(self, in_or_out, path_prefix=None, path_suffix="store=android"):
        pass

    def get_raw_ios_app_info(self, in_or_out, path_prefix=None, path_suffix="store=itunes"):
        pass

    def get_android_apps_to_scrape(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ww_downloads_to_scrape'], path_suffix, in_or_out)

    # Store analysis
    def get_android_store_ranks(self, in_or_out, path_prefix=None, path_suffix="store=0"):
        return self.__create_app_path_object(self.__get_scraping_base(path_prefix),
                                             self.apps_paths['store_ranks'], path_suffix, in_or_out)

    def get_ios_store_ranks(self, in_or_out, path_prefix=None, path_suffix="store=1"):
        return self.__create_app_path_object(self.__get_scraping_base(path_prefix),
                                             self.apps_paths['store_ranks'], path_suffix, in_or_out)

    # # Version DB
    def get_google_play_version_db(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_version_db'], path_suffix, in_or_out)

    def get_ios_app_store_version_db(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_version_db'], path_suffix, in_or_out)

    def get_google_play_timeline_db(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_timeline_db'], path_suffix, in_or_out)

    def get_ios_app_store_timeline_db(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_timeline_db'], path_suffix, in_or_out)

    def get_ga4_raw(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_store_ga4_raw'], path_suffix, in_or_out)

    def get_ga4_clean_table(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_store_ga4_clean_table'], path_suffix, in_or_out)

    def get_ga4_recognize_full_table(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_store_ga4_recognize_full'], path_suffix, in_or_out)

    def get_ga4_recognize_missings(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_store_ga4_recognize_missings'], path_suffix, in_or_out)

    def get_ga4_recognize_db_missings(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_store_ga4_recognize_db'], path_suffix, in_or_out)

    def get_ga4_filling_full_table(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_store_ga4_filling_table'], path_suffix, in_or_out)

    def get_ga4_processed_ls(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ga4_processed_ls'], path_suffix, in_or_out)

    def get_version_db_dump(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['version_db_dump'], path_suffix, in_or_out)

    # # Reviews

    # # Format Strings

    CAT_PARTITION = "category=%s"
    TOPIC_PARTITION = "topic=%s"
    CAT_TOPIC_PARTITION = "category=%s/topic=%s"

    STORE_ERROR = "%s : is not a recognized store code"

    # # # Data Paths

    # # # # General

    def get_google_play_raw_reviews(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_raw_reviews'], path_suffix, in_or_out)

    def get_ios_app_store_raw_reviews(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_raw_reviews'], path_suffix, in_or_out)

    def get_raw_reviews(self,  in_or_out, store, path_prefix=None, path_suffix=None):
        if store == GOOGLE_PLAY:
            return self.get_google_play_raw_reviews(in_or_out, path_prefix, path_suffix)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_store_raw_reviews(in_or_out, path_prefix, path_suffix)
        else:
            raise ValueError(self.STORE_ERROR % store)

    def get_google_play_preprocessed_reviews(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_preprocessed_reviews'], path_suffix, in_or_out)

    def get_ios_app_store_preprocessed_reviews(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_preprocessed_reviews'], path_suffix, in_or_out)

    def get_preprocessed_reviews(self, in_or_out, store, category=None, path_prefix=None):

        path_suffix = self.CAT_PARTITION % category if category else ""

        if store == GOOGLE_PLAY:
            return self.get_google_play_preprocessed_reviews(in_or_out, path_prefix, path_suffix)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_store_preprocessed_reviews(in_or_out, path_prefix, path_suffix)
        else:
            raise ValueError(self.STORE_ERROR % store)

    def get_google_play_analyzed_reviews(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_analyzed_reviews'], path_suffix, in_or_out)

    def get_ios_app_store_analyzed_reviews(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_analyzed_reviews'], path_suffix, in_or_out)

    def get_analyzed_reviews(self, in_or_out, store, category=None, path_prefix=None):

        path_suffix = self.CAT_PARTITION % category if category else ""

        if store == GOOGLE_PLAY:
            return self.get_google_play_analyzed_reviews(in_or_out, path_prefix, path_suffix)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_store_analyzed_reviews(in_or_out, path_prefix, path_suffix)
        else:
            raise ValueError(self.STORE_ERROR % store)

    # # # # Sentiment Analysis

    def get_google_play_reviews_sentiment(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_reviews_sentiment'], path_suffix, in_or_out)

    def get_ios_app_store_reviews_sentiment(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_reviews_sentiment'], path_suffix, in_or_out)

    def get_reviews_sentiment(self, in_or_out, store, path_prefix=None, path_suffix=None):
        if store == GOOGLE_PLAY:
            return self.get_google_play_reviews_sentiment(in_or_out, path_prefix, path_suffix)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_store_reviews_sentiment(in_or_out, path_prefix, path_suffix)
        else:
            raise ValueError(self.STORE_ERROR % store)

    # # # # Topic Analysis

    def get_google_play_nlp_transformed_reviews(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_nlp_transformed_reviews'], path_suffix, in_or_out)

    def get_ios_app_store_nlp_transformed_reviews(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_nlp_transformed_reviews'], path_suffix, in_or_out)

    def get_nlp_transformed_reviews(self,  in_or_out, store, category=None, path_prefix=None):

        path_suffix = self.CAT_PARTITION % category if category else ""

        if store == GOOGLE_PLAY:
            return self.get_google_play_nlp_transformed_reviews(in_or_out, path_prefix, path_suffix)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_store_nlp_transformed_reviews(in_or_out, path_prefix, path_suffix)
        else:
            raise ValueError(self.STORE_ERROR % store)

    def get_google_play_inferred_reviews(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_inferred_reviews'], path_suffix, in_or_out)

    def get_ios_app_store_inferred_reviews(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_inferred_reviews'], path_suffix, in_or_out)

    def get_inferred_reviews(self,  in_or_out, store, category=None, topic=None, path_prefix=None):

        if category and topic:
            path_suffix = self.CAT_TOPIC_PARTITION % (category, topic)
        elif category:
            path_suffix = self.CAT_PARTITION % category
        else:
            path_suffix = ""

        if store == GOOGLE_PLAY:
            return self.get_google_play_inferred_reviews(in_or_out, path_prefix, path_suffix)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_store_inferred_reviews(in_or_out, path_prefix, path_suffix)
        else:
            raise ValueError(self.STORE_ERROR % store)

    def get_google_play_reviews_topic_analysis(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_reviews_topic_analysis'], path_suffix, in_or_out)

    def get_ios_app_store_reviews_topic_analysis(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_reviews_topic_analysis'], path_suffix, in_or_out)

    def get_reviews_topic_analysis(self,  in_or_out, store, category=None, path_prefix=None):

        path_suffix = self.CAT_PARTITION % category if category else ""

        if store == GOOGLE_PLAY:
            return self.get_google_play_reviews_topic_analysis(in_or_out, path_prefix, path_suffix)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_store_reviews_topic_analysis(in_or_out, path_prefix, path_suffix)
        else:
            raise ValueError(self.STORE_ERROR % store)

    def get_google_play_reviews_per_app(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_reviews_per_app'], path_suffix, in_or_out)

    def get_ios_app_store_reviews_per_app(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_reviews_per_app'], path_suffix, in_or_out)

    def get_reviews_per_app(self, in_or_out, store, category=None, path_prefix=None):

        path_suffix = self.CAT_PARTITION % category if category else ""

        if store == GOOGLE_PLAY:
            return self.get_google_play_reviews_per_app(in_or_out, path_prefix, path_suffix)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_store_reviews_per_app(in_or_out, path_prefix, path_suffix)
        else:
            raise ValueError(self.STORE_ERROR % store)

    # # # Model Paths

    # # # # Topic Analysis

    def get_google_play_reviews_nlp_pipeline(self, category, path_prefix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base('in', path_prefix),
                                      self.apps_paths['google_play_reviews_nlp_pipeline'],
                                      self.CAT_PARTITION % category, 'in')

    def get_ios_app_store_reviews_nlp_pipeline(self, category, path_prefix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base('in', path_prefix),
                                      self.apps_paths['ios_app_store_reviews_nlp_pipeline'],
                                      self.CAT_PARTITION % category, 'in')

    def get_reviews_nlp_pipeline(self, store, category=None):
        if not category:
            raise ValueError('Please specify category')
        if store == GOOGLE_PLAY:
            return self.get_google_play_reviews_nlp_pipeline(category)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_store_reviews_nlp_pipeline(category)
        else:
            raise ValueError(self.STORE_ERROR % store)

    def get_google_play_reviews_inference_model(self, category, topic, path_prefix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base('in', path_prefix),
                                      self.apps_paths['google_play_reviews_inference_model'],
                                      self.CAT_TOPIC_PARTITION % (category, topic), 'in')

    def get_ios_app_store_reviews_inference_model(self, category, topic, path_prefix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base('in', path_prefix),
                                      self.apps_paths['ios_app_store_reviews_inference_model'],
                                      self.CAT_TOPIC_PARTITION % (category, topic), 'in')

    def get_reviews_inference_model(self, store, category=None, topic=None):
        if not (category and topic):
            raise ValueError('Please specify category and topic')
        if store == GOOGLE_PLAY:
            return self.get_google_play_reviews_inference_model(category, topic)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_store_reviews_inference_model(category, topic)
        else:
            raise ValueError(self.STORE_ERROR % store)

    # # Ratings

    def get_google_play_ratings(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['google_play_ratings'], path_suffix, in_or_out)

    def get_ios_app_store_ratings(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_store_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_app_store_ratings'], path_suffix, in_or_out)

    def get_ratings(self, in_or_out, store, path_prefix=None, path_suffix=None):
        if store == GOOGLE_PLAY:
            return self.get_google_play_ratings(in_or_out, path_prefix, path_suffix)
        elif store == IOS_APP_STORE:
            return self.get_ios_app_store_ratings(in_or_out, path_prefix, path_suffix)
        else:
            raise ValueError("%s : is not a recognized store code" % store)

    # Static Paths
    def get_countries_full_names(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_mobile_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['countries_full_names'], path_suffix, in_or_out)
    def get_z_norm_dist(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['z_norm_dist'], path_suffix, in_or_out)
    def get_static_matching_predict(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['static_matching_predict'], path_suffix, in_or_out)

    def get_new_static_matching_predict(self, in_or_out, path_prefix=None, path_suffix=None):
            return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                                 self.apps_paths['new_static_matching_predict'], path_suffix, in_or_out)

    def get_static_affinity(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['static_affinity'], path_suffix, in_or_out)

    def get_static_mobidays_usage_fix(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                         self.apps_paths['static_mobidays_usage_fix'], path_suffix, in_or_out)

    def get_app_panel(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_panel'], path_suffix, in_or_out)

    def get_country_panel(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['country_panel'], path_suffix, in_or_out)

    def get_app_affinity(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_affinity'], path_suffix, in_or_out)

    def get_app_affinity_with_ww(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_affinity_with_ww'], path_suffix, in_or_out)


    def get_app_affinity_pairs(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_affinity_pairs'], path_suffix, in_or_out)

    def get_app_affinity_pairs_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_affinity_pairs_agg'], path_suffix, in_or_out)

    def get_app_scores(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['app_scores'], path_suffix, in_or_out)

    def get_app_scores_with_info(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                                 self.apps_paths['app_scores_with_info'], path_suffix, in_or_out)

    def get_category_ranks(self, in_or_out, path_prefix=None, path_suffix=None, os='android'):
        self.apps_paths['category_ranks']['size'] = self.get_category_ranks_required_size(os)
        return self.__create_app_path_object(self.__get_os_base(in_or_out, path_prefix, os),
                                                 self.apps_paths['category_ranks'], path_suffix, in_or_out)

    def get_category_ranks_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                         self.apps_paths['category_ranks_parquet'], path_suffix, in_or_out)

    def get_store_category_ranks(self, in_or_out, path_prefix=None, path_suffix=None, os='android'):
        self.apps_paths['store_category_ranks']['size'] = self.get_store_category_ranks_required_size(os)
        return self.__create_app_path_object(self.__get_os_base(in_or_out, path_prefix, os),
                                         self.apps_paths['store_category_ranks'], path_suffix, in_or_out)

    def get_trending_apps(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['trending_apps'], path_suffix, in_or_out)

    def get_trending_apps_parquet(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['trending_apps_parquet'], path_suffix, in_or_out)

    def get_usage_climbing_apps(self, in_or_out, path_prefix=None, path_suffix=None, td=0):
        self.apps_paths['usage_climbing_apps']['size'] = self.get_usage_climbing_apps_required_size(td)
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_climbing_apps'], path_suffix, in_or_out)

    def get_usage_slipping_apps(self, in_or_out, path_prefix=None, path_suffix=None, td=0):
        self.apps_paths['usage_slipping_apps']['size'] = self.get_usage_slipping_apps_required_size(td)
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_slipping_apps'], path_suffix, in_or_out)

    def get_usage_ranking_apps(self, in_or_out, path_prefix=None, path_suffix=None, td=0):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['usage_ranking_apps'], path_suffix, in_or_out)

    def get_usage_climbing_apps_required_size(self, td):
        sizes = {
            7: 700 * KB,
            28: 1.8 * MB,
            1: 2.4 * MB
        }
        return sizes.get(td, 0)

    def get_usage_slipping_apps_required_size(self, td):
        sizes = {
            7: 800 * KB,
            28: 1.9 * MB,
            1: 2.5 * MB
        }
        return sizes.get(td, 0)

    def get_category_ranks_required_size(self, os):
        sizes = {
            'android': 100 * MB,
            'ios': 8 * MB
        }
        return sizes.get(os, 0)

    def get_store_category_ranks_required_size(self, os):
        sizes = {
            'android': 140 * MB,
            'ios': 12 * MB
        }
        return sizes.get(os, 0)

    #retention
    def get_aggregated_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['aggregated_retention'], path_suffix, in_or_out)

    def get_aggregated_retention_before_mobidays(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['aggregated_retention_before_mobidays'], path_suffix, in_or_out)

    def get_preprocess_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['preprocess_retention'], path_suffix, in_or_out)

    def get_calc_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['calc_retention'], path_suffix, in_or_out)

    def get_ww_smoothing_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ww_smoothing_retention'], path_suffix, in_or_out)

    def get_category_smoothing_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['category_smoothing_retention'], path_suffix, in_or_out)

    def get_top_app_smoothing_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['top_app_smoothing_retention'], path_suffix, in_or_out)

    def get_final_prior_smoothing_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['final_prior_smoothing_retention'], path_suffix, in_or_out)

    def get_estimated_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['estimated_retention'], path_suffix, in_or_out)

    def get_categories_estimated_retention(self, in_or_out, path_prefix=None, path_suffix=None):
            return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                                 self.apps_paths['categories_estimated_retention'], path_suffix, in_or_out)

    #install_retention
    def get_aggregated_install_retention_1001(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['aggregated_install_retention_1001'], path_suffix, in_or_out)
    def get_aggregated_install_retention_1005(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                            self.apps_paths['aggregated_install_retention_1005'], path_suffix, in_or_out)

    def get_preprocess_install_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['preprocess_install_retention'], path_suffix, in_or_out)

    def get_calc_install_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['calc_install_retention'], path_suffix, in_or_out)

    def get_ww_smoothing_install_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ww_smoothing_install_retention'], path_suffix, in_or_out)

    def get_category_smoothing_install_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['category_smoothing_install_retention'], path_suffix, in_or_out)

    def get_top_app_smoothing_install_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['top_app_smoothing_install_retention'], path_suffix, in_or_out)

    def get_final_prior_smoothing_install_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['final_prior_smoothing_install_retention'], path_suffix, in_or_out)

    def get_estimated_install_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['estimated_install_retention'], path_suffix, in_or_out)

    def get_categories_estimated_install_retention(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['categories_estimated_install_retention'], path_suffix, in_or_out)

    def get_stay_focus_installed_data(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['stay_focus_installed_apps'], path_suffix, in_or_out)

    def get_stay_focus_apps_sessions(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['stay_focus_apps_sessions'], path_suffix, in_or_out)

    def get_usage_patterns_session_list(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_session_list'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)

    def get_usage_patterns_raw_estimation(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_raw_estimation'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)


    def get_usage_patterns_source_raw_estimation(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_source_raw_estimation'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)


    def get_usage_patterns_weeks_merged_estimation(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_weeks_merged'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)

    def get_usage_patterns_estimation_with_ww(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_estimation_with_ww'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)

    def get_usage_patterns_prior_estimation(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_prior_estimation'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)

    def get_usage_patterns_category_estimation(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_category_estimation'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)

    def get_usage_patterns_fused_prior_estimation(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_fused_prior_estimation'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)

    def get_usage_patterns_final_estimation(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_final_estimation'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)

    def get_usage_patterns_stability_estimation(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_stability_estimation'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)

    def get_usage_patterns_clean_for_monitoring(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_clean_for_monitoring'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)

    def get_usage_patterns_final_estimation_snapshot(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_final_estimation_snapshot'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)

    def get_usage_patterns_category_final_estimation_snapshot(self, in_or_out, path_prefix=None, path_suffix=None, mode="DOW"):
        apps_paths = copy.deepcopy(self.apps_paths['usage_patterns_category_final_estimation_snapshot'])
        apps_paths['main_path'] = apps_paths['main_path'].format(mode=mode.lower())
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             apps_paths, path_suffix, in_or_out)

    def get_country_workweek_days(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['country_workweek_days'], path_suffix, in_or_out)
    #demographics
    def get_raw_stats_mobile(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['raw_stats_mobile'], path_suffix, in_or_out)

    def get_user_info(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['user_info'], path_suffix, in_or_out)

    def get_age_model(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['age_model'], path_suffix, in_or_out)

    def get_gender_model(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['gender_model'], path_suffix, in_or_out)

    def get_apps_all_age(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['apps_all_age'], path_suffix, in_or_out)

    def get_apps_all_gender(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['apps_all_gender'], path_suffix, in_or_out)

    def get_apps_country_age(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['apps_country_age'], path_suffix, in_or_out)

    def get_apps_country_gender(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['apps_country_gender'], path_suffix, in_or_out)

    def get_apps_all_age_aggregate(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['apps_all_age_aggregate'], path_suffix, in_or_out)

    def get_apps_all_gender_aggregate(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['apps_all_gender_aggregate'], path_suffix, in_or_out)
    def get_apps_country_age_aggregate(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['apps_country_age_aggregate'], path_suffix, in_or_out)

    def get_apps_country_gender_aggregate(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['apps_country_gender_aggregate'], path_suffix, in_or_out)

    #ios mau
    def get_new_apps_matching_static(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['new_apps_matching_static'], path_suffix, in_or_out)

    def get_new_ios_conv(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                                 self.apps_paths['new_ios_conv'], path_suffix, in_or_out)

    def get_old_coverage_static(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_base_dir(in_or_out, path_prefix),
                                             self.apps_paths['old_coverage_static'], path_suffix, in_or_out)

    def get_ios_features_10(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_ios_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_features_10'], path_suffix, in_or_out)

    def get_ios_features_11(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_ios_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['ios_features_11'], path_suffix, in_or_out)

    def get_downloads_raw_app_country_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['get_downloads_raw_app_country_country_source_agg'], path_suffix, in_or_out)

    def get_static_mobidays_fix_downloads_app_country_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['get_static_mobidays_fix_downloads_app_country_country_source_agg'], path_suffix, in_or_out)


    def get_dau_raw_app_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['get_dau_raw_app_country_source_agg'], path_suffix, in_or_out)

    def get_static_mobidays_fix_app_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['get_static_mobidays_fix_app_country_source_agg'], path_suffix, in_or_out)

    def get_dau_raw_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['get_dau_raw_country_source_agg'], path_suffix, in_or_out)

    def get_static_mobidays_fix_country_source_agg(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['get_static_mobidays_fix_country_source_agg'], path_suffix, in_or_out)

    def get_static_mobidays_retention_fix(self, in_or_out, path_prefix=None, path_suffix=None):
        return self.__create_app_path_object(self.__get_android_apps_analytics_base(in_or_out, path_prefix),
                                             self.apps_paths['static_mobidays_retention_fix'], path_suffix, in_or_out)

