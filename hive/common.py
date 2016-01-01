# ! /usr/bin/env python
import logging
import os
import subprocess
import random
import calendar
from datetime import *
from os.path import isfile, join
from os import listdir
from urlparse import urlparse

from dateutil.relativedelta import relativedelta
import sys

GLOBAL_DRYRUN = False  # This is crazy ugly, should allow executor to deploy jars


class ContextFilter(logging.Filter):
    CURRENT_LOG_NUM = 1

    def filter(self, record):
        record.count = self.CURRENT_LOG_NUM
        self.CURRENT_LOG_NUM += 1
        return True


logging.basicConfig(format='%(asctime)s [ %(process)s : %(count)s ] %(filename)s %(levelname)s %(message)s',
                    # datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG,
                    stream=sys.stdout)
logger = logging.getLogger(os.path.basename(__file__))
logger.addFilter(ContextFilter())

MOBILE_ALL_CATEGORY = '\"\"'
UNRANKED = -1

quality_count_writable = "sum(a.visits),sum(a.pageviews),sum(a.onepagevisits),sum(a.timeonsite)"
quality_estimated_count_writable = "sum(a.visits), sum(a.estimatedvisits),sum(a.pageviews),sum(a.estimatedpageviews),sum(a.onepagevisits),sum(a.timeonsite)"
unique_visits_pvs_count_writable = "sum(a.visits),sum(a.pageviews),sum(a.onepagevisits),sum(a.timeonsite),sum(a.unique)"
page_views_count_writable = "sum(a.pageviews)"
daily_files_location = "/similargroup/data/analytics/daily/agg"
outliers_blacklist_location = "/similargroup/data/analytics/resources/outliersblacklist"

daily_tables = {  # "incoming-data": "analytics.daily_incoming_data",
                  "incoming-keywords": "analytics.daily_incoming_keywords",
                  "incoming": "analytics.daily_incoming",
                  "outgoing": "analytics.daily_outgoing",
                  "social-receiving": "analytics.daily_social_receiving",
                  "sending-pages": "analytics.daily_sending_pages",
                  "popular-pages": "analytics.daily_popular_pages",
                  "raw-site-country-source": "analytics.daily_raw_site_country_source",
                  "raw-country-source": "analytics.daily_raw_country_source", "sr-estimate": "analytics.daily_incoming",
                  "estimated-values": "analytics.daily_estimated_values",
                  "special-referrer": "analytics.daily_incoming",
                  "app-source": "analytics.daily_apps_data"}

daily_table_names = {  # "incoming-data": "analytics.daily_incoming_data",
                       "incoming-keywords": "/similargroup/data/analytics/daily/aggregation/aggkey=incoming-keywords",
                       "incoming": "/similargroup/data/analytics/daily/aggregation/aggkey=incoming",
                       "outgoing": "/similargroup/data/analytics/daily/aggregation/aggkey=outgoing",
                       "social-receiving": "/similargroup/data/analytics/daily/aggregation/aggkey=social-receiving",
                       "sending-pages": "/similargroup/data/analytics/daily/aggregation/aggkey=sending-pages",
                       "popular-pages": "/similargroup/data/analytics/daily/aggregation/aggkey=raw-site-country-source",
                       "raw-site-country-source": "/similargroup/data/analytics/daily/aggregation/aggkey=raw-site-country-source",
                       "raw-country-source": "/similargroup/data/analytics/daily/aggregation/aggkey=raw-site-country-source",
                       "sr-estimate": "/similargroup/data/analytics/daily/aggregation/aggkey=incoming",
                       "estimated-values": "/similargroup/data/analytics/daily/post-estimate/estimate=values",
                       "special-referrer": "/similargroup/data/analytics/daily/aggregation/aggkey=incoming",
                       "app-source": "/similargroup/data/analytics/daily/aggregation/aggkey=app-source"}

window_tables = {  # "incoming-data": "analytics.window_incoming_data",
                   "incoming-keywords": "analytics.window_incoming_keywords", "incoming": "analytics.window_incoming",
                   "outgoing": "analytics.window_outgoing", "social-receiving": "analytics.window_social_receiving",
                   "sending-pages": "analytics.window_sending_pages", "popular-pages": "analytics.window_popular_pages",
                   "raw-site-country-source": "analytics.window_raw_site_country_source",
                   "raw-country-source": "analytics.window_raw_country_source",
                   "estimated-values": "analytics.window_estimated_values",
                   "sr-estimate": "analytics.window_sr_estimate",
                   "special-referrer": "analytics.window_special_referrers",
                   "app-source": "analytics.window_app_data"}

snapshot_tables = {  # "incoming-data-with-source": "analytics.snapshot_incoming_data_with_source",
                     # "incoming-data": "analytics.snapshot_incoming_data",
                     "incoming-keywords": "analytics.snapshot_incoming_keywords",
                     "incoming": "analytics.snapshot_incoming", "outgoing": "analytics.snapshot_outgoing",
                     "social-receiving": "analytics.snapshot_social_receiving",
                     "sending-pages": "analytics.snapshot_sending_pages",
                     "popular-pages": "analytics.snapshot_popular_pages",
                     "raw-site-country-source": "analytics.snapshot_raw_site_country_source",
                     "raw-country-source": "analytics.snapshot_raw_country_source",
                     "estimated-values": "analytics.snapshot_estimated_values",
                     "sr-estimate": "analytics.snapshot_sr_estimate", "app-source": "analytics.snapshot_app_data",
                     "feature-counts": "analytics.snapshot_features_counts",
                     "site-distros": "analytics.sites_distros",
                     "sites-info": "analytics.sitesinfo",
                     "target-counts": "/similargroup/data/analytics/snapshot/mobile-share/response/monthly-target-counts",
                     "qc-mobile-counts": "analytics.snapshot_mobile_target_monthly",
                     "special-referrer": "analytics.snapshot_special_referrers",
                     "app-source": "analytics.snapshot_app_data"}

snapshot_tables_names = {  # "incoming-data-with-source": "analytics.snapshot_incoming_data_with_source",
                           # "incoming-data": "analytics.snapshot_incoming_data",
                           "incoming-keywords": "/similargroup/data/analytics/snapshot/aggregation/aggkey=incoming-keywords",
                           "incoming": "/similargroup/data/analytics/snapshot/aggregation/aggkey=incoming",
                           "outgoing": "/similargroup/data/analytics/snapshot/aggregation/aggkey=outgoing",
                           "social-receiving": "/similargroup/data/analytics/snapshot/aggregation/aggkey=social-receiving",
                           "sending-pages": "/similargroup/data/analytics/snapshot/aggregation/aggkey=sending-pages",
                           "popular-pages": "/similargroup/data/analytics/snapshot/aggregation/aggkey=popular-pages",
                           "raw-site-country-source": "/similargroup/data/analytics/snapshot/aggregation/aggkey=raw-site-country-source",
                           "raw-country-source": "/similargroup/data/analytics/snapshot/aggregation/aggkey=raw-country-source",
                           "estimated-values": "/similargroup/data/analytics/snapshot/post-estimate/aggkey=estimate-values",
                           "sr-estimate": "/similargroup/data/analytics/snapshot/aggregation/aggkey=sr-estimate",
                           "app-source": "/similargroup/data/analytics/snapshot/aggregation/aggkey=app-source",
                           "feature-counts": "/similargroup/data/analytics/snapshot/mobile-share/features/features-counts",
                           "site-distros": "/similargroup/data/analytics/snapshot/general/sites-distro/type=monthly",
                           "sites-info": "/similargroup/data/analytics/snapshot/general/sitesinfo/type=monthly",
                           "target-counts": "/similargroup/data/analytics/snapshot/mobile-share/response/monthly-target-counts",
                           "qc-mobile-counts": "/similargroup/data/analytics/estimation/learningSet/country=ALL",
                           "special-referrer": "/similargroup/data/analytics/snapshot/aggregation/aggkey=special-referrer",
                           "app-source": "/similargroup/data/analytics/snapshot/aggregation/aggkey=app-source"}

mobile_share_tables =  {"feature-counts": {"name": "analytics.snapshot_features_counts", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/features-counts"},
                        "feature-refs": {"name": "analytics.snapshot_features_refs", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/refs"},
                        "feature-refs-ratios": {"name": "analytics.snapshot_features_refs_ratios", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/refs-ratios"},
                        "feature-tos-bounce-monthly": {"name": "analytics.snapshot_features_time_os_and_bounce_monthly", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/tos-and-bounce-monthly"},
                        "feature-tos-bounce": {"name": "analytics.snapshot_features_time_os_and_bounce", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/tos-and-bounce"},
                        "feature-categories": {"name": "analytics.snapshot_features_categories", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/main-category"},
                        "feature-ranks": {"name": "analytics.snapshot_features_ranks", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/ranks"},
                        "target-counts": {"name": "analytics.snapshot_target_counts_monthly", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/response/monthly-target-counts"},
                        "feature-combined": {"name": "analytics.snapshot_combined_features_table", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/combined-features"},
                        "combined-features-target": {"name": "analytics.snapshot_combined_feat_target_table", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/combined-features-combined"}}

mobile_share_tables =  {"feature-counts": {"name": "analytics.snapshot_features_counts", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/features-counts"},
                        "feature-refs": {"name": "analytics.snapshot_features_refs", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/refs"},
                        "feature-refs-ratios": {"name": "analytics.snapshot_features_refs_ratios", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/refs-ratios"},
                        "feature-tos-bounce-monthly": {"name": "analytics.snapshot_features_time_os_and_bounce_monthly", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/tos-and-bounce-monthly"},
                        "feature-tos-bounce": {"name": "analytics.snapshot_features_time_os_and_bounce", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/tos-and-bounce"},
                        "feature-categories": {"name": "analytics.snapshot_features_categories", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/main-category"},
                        "feature-ranks": {"name": "analytics.snapshot_features_ranks", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/features/ranks"},
                        "target-counts": {"name": "analytics.snapshot_target_counts_monthly", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/response/monthly-target-counts"},
                        "feature-combined": {"name": "analytics.snapshot_combined_features_table", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/combined-features"},
                        "combined-features-target": {"name": "analytics.snapshot_combined_feat_target_table", "base_path": "/similargroup/data/analytics/snapshot/mobile-share/combined-features-combined"}}

snapshot_tables_sum_filters = {  # "incoming-data": "",
                                 "incoming-keywords": " and (keywords != '' and keywords is not null)",
                                 "incoming": " and site2 != '' and site2 is not null and (refid != 6 or (refid = 6 and issitereferral = 1))",
                                 "outgoing": "",
                                 "social-receiving": "",
                                 "sending-pages": "",
                                 "popular-pages": "",
                                 "raw-site-country-source": "",
                                 "raw-country-source": "",
                                 "estimated-values": "",
                                 "sr-estimate": "",
                                 "special-referrer": "",
                                 "app-source": ""}

window_estimate_params_tables = {  # "incoming-data": "analytics.window_incoming_data_estimate_params",
                                   "incoming-keywords": "analytics.window_incoming_keywords_estimate_params",
                                   "incoming": "analytics.window_incoming_estimate_params",
                                   "outgoing": "analytics.window_outgoing_estimate_params",
                                   "social-receiving": "analytics.window_social_receiving_estimate_params",
                                   "sending-pages": "analytics.window_sending_pages_estimate_params",
                                   "sr-estimate": "analytics.window_sr_estimate_params"}

snapshot_estimate_params_tables = {  # "incoming-data": "analytics.snapshot_incoming_data_estimate_params",
                                     "incoming-keywords": "analytics.snapshot_incoming_keywords_estimate_params",
                                     "incoming": "analytics.snapshot_incoming_estimate_params",
                                     "outgoing": "analytics.snapshot_outgoing_estimate_params",
                                     "social-receiving": "analytics.snapshot_social_receiving_estimate_params",
                                     "sending-pages": "analytics.snapshot_sending_pages_estimate_params",
                                     "sr-estimate": "analytics.snapshot_sr_estimate_params"}

window_estimated_values_tables = {  # "incoming-data": "analytics.window_estimated_values",
                                    "incoming-keywords": "analytics.window_temp_sending_pages_estimated",
                                    "incoming": "analytics.window_temp_sending_pages_estimated",
                                    "outgoing": "analytics.window_estimated_values",
                                    "social-receiving": "analytics.window_temp_social_estimated",
                                    "sending-pages": "analytics.window_temp_sending_pages_estimated",
                                    "sr-estimate": "analytics.window_estimated_values",
                                    "app-source": "analytics.window_estimated_values"}

snapshot_estimated_values_tables = {"social-receiving": "analytics.snapshot_temp_social_estimated",
                                    # was "analytics.snapshot_temp_social_estimated",
                                    "incoming-keywords": "analytics.snapshot_temp_sending_pages_estimated",
                                    "incoming": "analytics.snapshot_temp_sending_pages_estimated",
                                    "sending-pages": "analytics.snapshot_temp_sending_pages_estimated",
                                    # "incoming-data"    : "analytics.snapshot_estimated_values", # should be removed (was one-collector)
                                    "outgoing": "analytics.snapshot_estimated_values",  # broken
                                    "app-source": "analytics.snapshot_estimated_values",
                                    "sr-estimate": "analytics.snapshot_estimated_values"}

tables_with_outliers = {  # "incoming-data": False,
                          "incoming-keywords": False,
                          "incoming": True,
                          "outgoing": True,
                          "social-receiving": False,
                          "sending-pages": False,
                          "popular-pages": False,
                          "raw-site-country-source": False,
                          "raw-country-source": False,
                          "sr-estimate": False}

tables_estimated_by_sr = {  # "incoming-data": False,
                            "incoming-keywords": True,
                            "incoming": True,
                            "outgoing": False,
                            "social-receiving": False,
                            "sending-pages": True,
                            "sr-estimate": False}

window_estimated_tables = {  # "incoming-data": "analytics.window_estimated_incoming_data",
                             "incoming-keywords": "analytics.window_estimated_incoming_keywords",
                             "incoming": "analytics.window_estimated_incoming",
                             "outgoing": "analytics.window_estimated_outgoing",
                             "social-receiving": "analytics.window_estimated_social_receiving",
                             "sending-pages": "analytics.window_estimated_sending_pages",
                             "sr-estimate": "analytics.window_estimated_sr",
                             "app-source": "analytics.window_estimated_app_data"}

window_estimated_with_totals_tables = {  # "incoming-data": "analytics.window_estimated_totals_incoming_data",
                                         "incoming-keywords": "analytics.window_estimated_totals_incoming_keywords",
                                         "incoming": "analytics.window_estimated_totals_incoming",
                                         "outgoing": "analytics.window_estimated_totals_outgoing",
                                         "social-receiving": "analytics.window_estimated_totals_social_receiving",
                                         "sending-pages": "analytics.window_estimated_totals_sending_pages"}

snapshot_estimated_tables = {  # "incoming-data": "analytics.snapshot_estimated_incoming_data",
                               "incoming-keywords": "analytics.snapshot_estimated_incoming_keywords",
                               "incoming": "analytics.snapshot_estimated_incoming",
                               "outgoing": "analytics.snapshot_estimated_outgoing",
                               "social-receiving": "analytics.snapshot_estimated_social_receiving",
                               "sending-pages": "analytics.snapshot_estimated_sending_pages",
                               "sr-estimate": "analytics.snapshot_estimated_sr",
                               "app-source": "analytics.snapshot_estimated_app_data"}

snapshot_estimated_with_totals_tables = {  # "incoming-data": "analytics.snapshot_estimated_totals_incoming_data",
                                           "incoming-keywords": "analytics.snapshot_estimated_totals_incoming_keywords",
                                           "incoming": "analytics.snapshot_estimated_totals_incoming",
                                           "outgoing": "analytics.snapshot_estimated_totals_outgoing",
                                           "social-receiving": "analytics.snapshot_estimated_totals_social_receiving",
                                           "sending-pages": "analytics.snapshot_estimated_totals_sending_pages"}

tables_cols = {
    # "incoming-data": "site string, country int, refid int, refflag int, referralsite string, keywords string, sendingpage string, landingpage string, issitereferral int, visits double, pageviews double, onepagevisits double, timeonsite double",
    "incoming-keywords": "site string, country int, refid int, refflag int, keywords string, visits double, pageviews double, onepagevisits double, timeonsite double",
    "incoming": "site string, country int, refid int, refflag int, site2 string, issitereferral boolean, visits double, pageviews double, onepagevisits double, timeonsite double",
    "outgoing": "site string, country int, refid int, refflag int, site2 string, issitereferral boolean, visits double, pageviews double, onepagevisits double, timeonsite double",
    "social-receiving": "site string, country int, refid int, refflag int, page string, visits double, pageviews double, onepagevisits double, timeonsite double",
    "sending-pages": "site string, country int, refid int, refflag int, page string, visits double, pageviews double, onepagevisits double, timeonsite double",
    "popular-pages": "site string, country int, page string, pageviews double",
    "raw-site-country-source": "site string, country int, sourceId int, visits double, pageviews double, onepagevisits double, timeonsite double,unique double",
    "raw-country-source": "country int, sourceId int, visits double, pageviews double, onepagevisits double, timeonsite double,unique double"}

tables_key_cols = {
    # "incoming-data": "a.site, a.country, a.refid, a.refflag, a.referralsite, a.keywords, a.sendingpage, a.landingpage, a.issitereferral",
    "incoming-keywords": "a.site, a.country, a.refid, a.refflag, a.keywords",
    "incoming": "a.site, a.country, a.refid, a.refflag, a.site2, a.issitereferral",
    "outgoing": "a.site, a.country, a.refid, a.refflag, a.site2, a.issitereferral",
    "social-receiving": "a.site, a.country, a.refid, a.refflag, a.page",
    "sending-pages": "a.site, a.country, a.refid, a.refflag, a.page", "popular-pages": "a.site, a.country, a.page",
    "raw-site-country-source": "a.site, a.country, a.sourceId", "raw-country-source": "a.country, a.sourceId",
    "sr-estimate": "a.site, a.country, a.refid, a.refflag, a.site2, a.issitereferral",
    "special-referrer": "a.site, a.country, a.refid, a.refflag", #FIXME: max on refflag is a temporary fix for bad incoming collectors outputing a non paid special ref flag
    "app-source": "a.storetype, a.appid, a.country, a.refid, a.refflag, a.keyword, a.refsite, a.refapp, a.refcategory, a.refchart, a.refrelease, a.refdeveloper"}

tables_count_writables = {"incoming-data-with-source": quality_count_writable,
                          "incoming-keywords": quality_count_writable,
                          "incoming": quality_count_writable,
                          "outgoing": quality_count_writable,
                          "social-receiving": quality_count_writable,
                          "sending-pages": quality_count_writable,
                          "popular-pages": page_views_count_writable,
                          "raw-site-country-source": unique_visits_pvs_count_writable,
                          "raw-country-source": unique_visits_pvs_count_writable,
                          "sr-estimate": quality_count_writable,
                          "special-referrer": quality_count_writable,
                          "app-source": page_views_count_writable}


def getPartitionString(mode, mode_type, year, month, day, **kwargs):
    if mode == "window" or mode_type == "weekly":
        partition_parts = "year=%s, month=%02d, day=%02d, type='%s'" % (year, month, day, mode_type)
    else:
        partition_parts = "year=%s, month=%02d, type='%s'" % (year, month, mode_type)

    for key, value in kwargs.iteritems():
        if value:
            partition_parts += ', %s=%s' % (key, value)
        else:
            partition_parts += ', %s' % key

    return '(%s)' % partition_parts


def getDatePartitionString(year, month, day=None, **kwargs):
    if day:
        partition_parts = "year=%s, month=%02d, day=%02d" % (year, month, day)
    else:
        partition_parts = "year=%s, month=%02d" % (year, month)

    for key, value in kwargs.iteritems():
        if value:
            partition_parts += ', %s=%s' % (key, value)
        else:
            partition_parts += ', %s' % key

    return '(%s)' % partition_parts


def getWhereString(table_prefix, mode, mode_type, year, month, day):
    if mode == "window" or mode_type == "weekly":
        return " %(table_prefix)syear=%(year)02d and %(table_prefix)smonth=%(month)02d and %(table_prefix)sday=%(day)02d and %(table_prefix)stype='%(mode_type)s'" % {
        'table_prefix': table_prefix,
        'year': year,
        'month': month,
        'day': day,
        'mode_type': mode_type}
    else:
        return " %(table_prefix)syear=%(year)02d and %(table_prefix)smonth=%(month)02d and %(table_prefix)stype='%(mode_type)s'" % {
            'table_prefix': table_prefix,
            'year': year,
            'month': month,
            'mode_type': mode_type}


def get_monthly_where(year, month, day=None):
    if day:
        return 'year=%02d and month=%02d and day=%02d ' % (year, month, day)
    else:
        return 'year=%02d and month=%02d ' % (year, month)


def get_range_where_clause(year, month, day, mode, mode_type):
    if int(year) < 100:
        end_date = datetime(2000 + int(year), int(month), int(day)).date()
    else:
        end_date = datetime(int(year), int(month), int(day)).date()
    start_date = ""

    if mode_type == "last-1":
        start_date = end_date - timedelta(days=0)
    elif mode_type == "last-7":
        start_date = end_date - timedelta(days=6)
    elif mode_type == "last-28":
        start_date = end_date - timedelta(days=27)
    elif mode_type == "last-30":
        start_date = end_date - timedelta(days=29)
    elif mode_type == "last-90":
        start_date = end_date - timedelta(days=89)
    elif mode_type == "weekly":
        start_date = end_date - timedelta(days=int(end_date.weekday()))
        end_date = start_date + timedelta(days=6)
    elif mode_type == "monthly":
        return ' (year = %02d and month = %02d) ' % (end_date.year % 100, month)
    elif mode_type == "quarterly":
        start_date = datetime(int(year), int(month) - ((int(month) - 1) % 3), 1).date()
        end_date = start_date + timedelta(days=63)
        return '(year=%02d and month <= %02d and month >= %02d' % (end_date.year % 100, str(start_date.month), str(end_date.month))
    elif mode_type == "annually":
        return " (year = %02d) " % (end_date.year % 100)

    return get_where_between_dates(start_date, end_date)

def get_day_range_where_clause(year, month, day, window_size):
    if int(year) < 100:
        end_date = datetime(2000 + int(year), int(month), int(day)).date()
    else:
        end_date = datetime(int(year), int(month), int(day)).date()

    return get_where_between_dates(end_date - timedelta(days=int(window_size) - 1), end_date)

def get_where_between_dates(start_date, end_date):
    where_clause = ""

    curr_date = start_date
    while True:
        if where_clause != "":
            where_clause += " or "
        where_clause += ' (year=%02d and month=%02d and day=%02d) ' % (curr_date.year % 100, curr_date.month, curr_date.day)
        curr_date = curr_date + timedelta(days=1)
        if curr_date > end_date:
            break

    return ' (%s) ' % where_clause


def get_month_range_where_clause(end_date, months_back):
    return ' or '.join(['(year=%02d and month=%02d)' % (parse_date(end_date + relativedelta(months=-x))[:2]) for x in range(0, months_back)])


def deploy_jar(deploy_path, jar_location):
    logger.info("copy jars to hdfs, location on hdfs: " + jar_location + " from local path: " + deploy_path)
    if GLOBAL_DRYRUN:
        'print dryrun set, not actually deploying'
        return

    subprocess.call(["hadoop", "fs", "-rm", "-r", jar_location])
    subprocess.call(["hadoop", "fs", "-mkdir", "-p", jar_location])
    subprocess.call(["hadoop", "fs", "-put", deploy_path + "/analytics.jar", jar_location + "/analytics.jar"])
    subprocess.call(
        ["hadoop", "fs", "-put", deploy_path + "/lib/common-1.0.jar", jar_location + "/common.jar"])


# use ['add jar %s' % jar for jar in detect_jars(...) ]
def detect_jars(deploy_path, lib_path="lib"):
    main_jars = [jar for jar in listdir(deploy_path) if isfile(join(deploy_path, jar)) and jar.endswith('.jar')]
    full_lib_path = join(deploy_path, lib_path)
    lib_jars = [jar for jar in listdir(full_lib_path) if isfile(join(full_lib_path, jar)) and jar.endswith('.jar')]

    return lib_jars + main_jars


def deploy_all_jars(deploy_path, jar_location, lib_path="lib"):
    logger.info("copy jars to hdfs, location on hdfs: " + jar_location + " from local path: " + deploy_path)
    if GLOBAL_DRYRUN:
        'print dryrun set, not actually deploying'
        return

    main_jars = [jar for jar in listdir(deploy_path) if isfile(join(deploy_path, jar)) and jar.endswith('.jar')]
    full_lib_path = join(deploy_path,lib_path)
    lib_jars = [jar for jar in listdir(full_lib_path) if isfile(join(full_lib_path, jar)) and jar.endswith('.jar')]

    subprocess.call(["hadoop", "fs", "-rm", "-r", jar_location])
    subprocess.call(["hadoop", "fs", "-mkdir", "-p", jar_location])
    subprocess.call(["bash", "-c", "hadoop fs -put %s/*.jar %s" % (deploy_path, jar_location)])
    subprocess.call(["bash", "-c", "hadoop fs -put %s/*.jar %s" % (full_lib_path, jar_location)])

    return lib_jars + main_jars


def wait_on_processes(processes):
    for p in processes:
        print p.communicate()


def table_location(table):
    cmd = ['hive', '-e', '"describe formatted %s;"' % table]
    output, _ = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE).communicate()
    for line in output.split("\n"):
        if "Location:" in line:
            return line.split("\t")[1]


def delete_path(path):
    subprocess.call(("hadoop", "fs", "-rm", "-r", path))


def temp_table_cmds_internal(orig_table_name, temp_root):
    table_name = '%s_temp_%s' % (orig_table_name, random.randint(10000, 99999))
    drop_cmd = '\nDROP TABLE IF EXISTS %s;\n' % table_name
    create_cmd = '''\n
                    CREATE EXTERNAL TABLE %(table_name)s
                    LIKE %(orig_table_name)s
                    LOCATION '%(temp_root)s';
                    USE %(db_name)s;
                    MSCK REPAIR TABLE %(short_table_name)s;
                    \n
                ''' % {'table_name': table_name,
                       'orig_table_name': orig_table_name,
                       'temp_root': temp_root,
                       'db_name': table_name.split('.')[0],
                       'short_table_name': table_name.split('.')[-1]}
    return table_name, drop_cmd, create_cmd


def should_create_external_table(orig_table_name, location):
    # Remove hdfs:// where needed for comparison
    if 'hdfs://' in location:
        location = urlparse(location).path
    table_loc = table_location(orig_table_name)
    table_loc = urlparse(table_loc).path
    return table_loc != location


def temp_table_cmds(orig_table_name, root):
    logger.info("Checking whether to create external table %s in location %s:" % (orig_table_name, root))
    if should_create_external_table(root):
        logger.info("Writing to an external table in the given location.")
        return temp_table_cmds_internal(orig_table_name, root)
    else:
        logger.info("Writing to the original table in place. The location which was passed is being discarded.")
        return orig_table_name, '', ''


def dedent(s):
    return '\n'.join([line.lstrip() for line in s.split('\n') if line.strip()])


def parse_date(dt):
    return int(str(dt.year)[-2:]), dt.month, dt.day


def jar_location(branch='master'):
    return "/similargroup/jars/%s/" % branch


def list_days(end_date, mode, mode_type):
    if mode == 'snapshot':
        if mode_type == 'monthly':
            wkday, numdays = calendar.monthrange(end_date.year, end_date.month)
            return [date(year=end_date.year, month=end_date.month, day=x + 1) for x in range(0, numdays)]
        elif mode_type == 'weekly':
            delta = timedelta(weeks=1)
        else:
            return None
    elif mode == 'window':
        if mode_type == 'last-30':
            delta = timedelta(days=30)
        elif mode_type == 'last-28':
            delta = timedelta(days=28)
        elif mode_type == 'last-7':
            delta = timedelta(days=7)
        else:
            return None
    else:
        return None

    return [end_date - timedelta(days=x) for x in range(0, delta.days)]


class Stage(object):
    def __init__(self, queries):
        self.queries = queries
    def __str__(self):
        return '\n\n'.join(['\n'.join(self.queries.items())])



