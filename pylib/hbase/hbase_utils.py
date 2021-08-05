import logging
from time import sleep
import happybase
from socket import error as socket_error
from pylib.config.SnowflakeConfig import SnowflakeConfig
logger = logging.getLogger(__name__)


def retries(max_attempts, interval, exceptions=None):
    def decorator(function):
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts+1):
                try:
                    return function(*args, **kwargs)
                except Exception as e:
                    if not all([issubclass(e.__class__, x) or isinstance(e, x) for x in exceptions]):
                        raise e
                    logger.error("Attempt %s/%s failed - error:%s" % (attempt, max_attempts, e))
                    if attempt == max_attempts:
                        raise e
                    sleep(interval)
        return wrapper
    return decorator


def get_hbase_table(table_name, cluster_name=None):
    cluster_name = cluster_name or SnowflakeConfig().get_service_name(service_name="hbase")
    return happybase.Connection(cluster_name, timeout=5*60*1000).table(table_name)


@retries(max_attempts=5, interval=5, exceptions=[socket_error])
def scan_hbase_table(table_name, cluster_name=None, **kwargs):
    tbl = get_hbase_table(table_name, cluster_name)
    return list(tbl.scan(**kwargs))


def validate_records_per_region(table_name, columns=None, minimum_regions_count=1, min_rows_per_region=None,
                                min_rows=10000, cluster_name=None):
    logger.info('checking validity of hbase table: %s' % table_name)
    tbl = get_hbase_table(table_name, cluster_name)
    regions = tbl.regions()

    if len(regions) < minimum_regions_count:
        logger.error('table: %s too few regions in table (%d < %d)' % (table_name, len(regions), minimum_regions_count))
        return False

    if len(regions) > 1:

        if sorted([r['start_key'] for r in regions]) != [r['start_key'] for r in regions]:
            logger.info("table: %s, regions keys are not sorted properly, %s " %
                        (table_name, [(i, r['start_key'], r['end_key']) for i, r in enumerate(regions)]))
            return False

        successive_keys = zip([r['end_key'] for r in regions[:-1]], [r['start_key'] for r in regions[1:]])
        if any([end_key != next_start_key for end_key, next_start_key in successive_keys]):
            logger.info("table: %s, regions keys are not continuous, %s " %
                        (table_name, [(i, r['start_key'], r['end_key']) for i, r in enumerate(regions)]))
            return False

    if not columns:
        logger.info("checking any column-family")
        if min_rows > 0:
            rows = scan_hbase_table(table_name=table_name, cluster_name=cluster_name, limit=min_rows)
            if len(rows) < min_rows:
                logger.info("table: %s, to few keys (%d < %d)" % (table_name, len(rows), min_rows))
                return False
        if min_rows_per_region:
            for r_index, region in enumerate(regions):
                logger.info("checking region: %s " % region)
                region_rows = scan_hbase_table(
                    table_name=table_name, cluster_name=cluster_name,
                    row_start=region['start_key'],
                    row_stop=region['end_key'],
                    limit=min_rows_per_region)
                logger.info("got %s rows" % (len(region_rows)))
                if len(region_rows) == 0 or (len(region_rows) < min_rows_per_region and region != regions[-1]):
                    # ignore last region, it can have few keys
                    logger.info("table: %s, to few keys (%d < %d)  in region: %s " % (table_name, len(region_rows), min_rows_per_region, region['name']))
                    return False
    else:
        for column in columns:
            logger.info("checking column-family: %s " % column)
            if min_rows > 0:
                rows = scan_hbase_table(table_name=table_name, cluster_name=cluster_name, columns=[column], limit=min_rows)
                if len(rows) < min_rows:
                    logger.info("table: %s, column: %s, to few keys (%d < %d)" % (table_name, column, len(rows), min_rows))
                    return False
            if min_rows_per_region:
                for r_index, region in enumerate(regions):
                    logger.info("checking region: %s " % region)
                    region_rows = scan_hbase_table(
                        table_name=table_name, cluster_name=cluster_name,
                        row_start=region['start_key'],
                        row_stop=region['end_key'],
                        columns=[column],
                        limit=min_rows_per_region)
                    logger.info("got %s rows" % (len(region_rows)))
                    if len(region_rows) < min_rows_per_region:
                        logger.info("table: %s, column: %s, to few keys (%d < %d)  in region: %s " % (table_name, column, len(region_rows), min_rows_per_region, region['name']))
                        return False
    return True


# social-ads-recieving
# traffic
if __name__ == "__main__":
    #
    tasks = [
        #server-analytics/scripts/monthly/qa/quality_distro_ptasks.py
      (  'export_search_console_data_from_hbase- Not in use since 2020-02 DAG:GA_KW_Validation', [
            validate_records_per_region(table_name="ga_webmaster_21_02", columns=['data'], minimum_regions_count=10, min_rows_per_region=10)
        ]),
        #server-analytics/scripts/monthly/ranks.py
        ('export_sites_info', [
            validate_records_per_region(table_name="sites_info_21_03", columns=['data'], minimum_regions_count=15),
            validate_records_per_region(table_name="sites_info_last-28_21_04_19", columns=['data'], minimum_regions_count=15)
        ]),
        #server-analytics/scripts/monthly/ranks.py
        ('save_sites_ranking - DEAD?  ', [
            validate_records_per_region(table_name="top_lists_21_03", columns=['info'], minimum_regions_count=15),#input
            validate_records_per_region(table_name="top_lists_last-28_21_04_19", columns=['info'], minimum_regions_count=15),#input

            validate_records_per_region(table_name="sites_info_21_03", columns=['data'], minimum_regions_count=15), #output
            validate_records_per_region(table_name="sites_info_last-28_21_04_19", columns=['data'], minimum_regions_count=15),#output

            validate_records_per_region(table_name="sites_stat_21_03", columns=['ranks'], minimum_regions_count=15),#output
            validate_records_per_region(table_name="sites_stat_last-28_21_04_19", columns=['ranks'], minimum_regions_count=15)#output
        ]),
        #server-analytics/scripts/monthly/ranks.py
        ('save_top_lists - DEAD?', [
            validate_records_per_region(table_name="top_lists_21_03", columns=['list', 'info'], minimum_regions_count=15), #output
            validate_records_per_region(table_name="top_lists_last-28_21_04_19", columns=['list', 'info'], minimum_regions_count=15),#output
        ]),
        #server-analytics/scripts/monthly/ranks.py
        ('write_sites_stat_2019', [
            validate_records_per_region(table_name="sites_stat_21_03"), #output
            validate_records_per_region(table_name="sites_stat_last-28_21_04_19"),#output
        ]),
        #server-analytics/scripts/monthly/ranks.py
        ('write_sites_info', [
            validate_records_per_region(table_name="sites_info_21_03"), #output
            validate_records_per_region(table_name="sites_info_last-28_21_04_19"),#output
        ]),
        #server-analytics/scripts/monthly/ranks.py
       ('export_rest', [
            validate_records_per_region(table_name="sites_info_21_03", columns=['data'], minimum_regions_count=15),
            validate_records_per_region(table_name="sites_info_last-28_21_04_19", columns=['data'], minimum_regions_count=15),
        ]),
        #server-analytics/scripts/monthly/ranks.py
        ('export_adult_redis - Not in use since 2020-09 DAG:Websites_AdultRedis_Deploy ', [
            validate_records_per_region(table_name="top_lists_21_03", columns=['list'], minimum_regions_count=15),
        ]),
        #server-analytics/scripts/monthly/ranks.py
        ('export_sites_table', [
            validate_records_per_region(table_name="sites"),
        ]),
        #top_keywords: server-db-loader/scripts/view/top_keywords.py
       ( 'top_keywords', [ # input
            validate_records_per_region(table_name="sites_stat_21_03", columns=['search-keywords']), # input
            validate_records_per_region(table_name="top_keywords_21_03", columns=['search-keywords']),  # output
        ]),
        #server-mobile/scripts/web/keywords/process_aux_data.py
       ('export_scraping_from_hbase - DEAD? ', [
           validate_records_per_region(table_name="keywords_scraping_21_03", columns=['mobile_search']),  # input
           validate_records_per_region(table_name="keywords_scraping_last-28_21_04_19", columns=['mobile_search']), # input

       ]),
        ('site_top_keywords', [
            validate_records_per_region(table_name="sites_stat_21_03"), # output
            validate_records_per_region(table_name="sites_stat_last-28_21_04_19"), # output
        ]),
        ('pageviews_write_hbase_analytics_rerun2019', [
            validate_records_per_region(table_name="desktop_pageviews_21_03"), # output
            validate_records_per_region(table_name="desktop_pageviews_last-28_21_04_19"), # output
        ]),
        ('pageviews_write_hbase_mobile_rerun2019', [
            validate_records_per_region(table_name="mobile_web_pageviews_21_03"), # output
            validate_records_per_region(table_name="mobile_web_pageviews_last-28_21_04_19"), # output
        ]),
        ('insert_mw_daily_data_to_hbase', [
            validate_records_per_region(table_name="mobile_web_stats_21_03", columns=['stats']), # output
            validate_records_per_region(table_name="mobile_web_stats_last-28_21_04_19", columns=['stats']), # output
        ]),
#server-mobile/scripts/demographics/android/apps_aggregate.py
        ('write_hbase', [
            validate_records_per_region(table_name="app_demographics_21_03"), # output
        ]),
#server-mobile/scripts/web/adjust_est.py
        ('write_hbase', [
            validate_records_per_region(table_name="mobile_web_stats_21_03", columns=['stats']), # output
            validate_records_per_region(table_name="mobile_web_stats_last-28_21_04_19", columns=['stats']), # output
        ]),
        #sw-demographics/scripts/features_tasks.py
        ('insert_feature_table', [
            validate_records_per_region(table_name="desktop_demographics_21_03"), # output
            validate_records_per_region(table_name="mobile_web_demographics_21_03"), # output
        ]),

        ('create_snapshot-Apps_Deploy_Window', [
            validate_records_per_region(table_name="app_sdk_stats_last-28_21_04_19"),
            validate_records_per_region(table_name="app_sdk_category_stats_last-28_21_04_19"),
            validate_records_per_region(table_name="app_sdk_category_lead_last-28_21_04_19"),
            validate_records_per_region(table_name="app_eng_rank_last-28_21_04_19"),
            validate_records_per_region(table_name="cat_mod_app_rank_last-28_21_04_19"),
        ]),
        ('create_snapshot-Apps_Deploy_Snapshot', [
            validate_records_per_region(table_name="app_sdk_stats_21_03"),
            validate_records_per_region(table_name="app_sdk_category_stats_21_03"),
            validate_records_per_region(table_name="app_sdk_category_lead_21_03"),
            validate_records_per_region(table_name="app_eng_rank_21_03"),
            validate_records_per_region(table_name="cat_mod_app_rank_21_03"),
            validate_records_per_region(table_name="app_demographics_21_03"),
        ]),
        ('create_snapshot-Websites_Deploy_Window', [
            validate_records_per_region(table_name="top_lists_last-28_21_04_19"),
            validate_records_per_region(table_name="sites_stat_last-28_21_04_19"),
            validate_records_per_region(table_name="sites_info_last-28_21_04_19"),
            validate_records_per_region(table_name="data_settings_websites_last-28_21_04_19"),
            validate_records_per_region(table_name="desktop_pageviews_last-28_21_04_19"),
        ]),
        ('create_snapshot-Websites_Deploy_Snapshot', [
            validate_records_per_region(table_name="top_lists_21_03"),
            validate_records_per_region(table_name="sites_stat_21_03"),
            validate_records_per_region(table_name="sites_info_21_03"),
            validate_records_per_region(table_name="keywords_stats_21_03"),
            validate_records_per_region(table_name="desktop_pageviews_21_03"),
            validate_records_per_region(table_name="industry_analysis_21_03"),
            validate_records_per_region(table_name="top_keywords_21_03"),
            validate_records_per_region(table_name="desktop_demographics_21_03"),
            validate_records_per_region(table_name="audience_interests_21_03"),
        ]),
        ('lior-test-mobile_web_stats_20_03', [
            validate_records_per_region(table_name="lior-test-mobile_web_stats_20_03"),
        ])
    ]


    print "Results"
    for k, v in tasks:
        print "\nptask:%s:" % k
        for x in v:
            print "\t%s => %s" % (k, x)
