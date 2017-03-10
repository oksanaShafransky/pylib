from pylib.sw_config.data import Artifact, Intersect

__author__ = 'Felix'

MINIMAL_VIABLE_DATES_SIZE = 2
DESRIED_DATES_SIZE = 3
LOOKBACK_SIZE = 10


class SimilarWebWindowConfig(object):
    def __init__(self):
        pass

    # for reflection based tools
    def iter_fields(self):
        return []

    @staticmethod
    def get_artifacts(proxy):
        desktop_window = Artifact(proxy, 'services/current-web-dates/window', required_value='true')
        mw_window = Artifact(proxy, 'services/current-mobile-web-dates/window', required_value='true')
        web_analysis = Intersect(desktop_window, mw_window)

        app_ranks = Artifact(proxy, 'services/mobile-usage-ranks/data-available/window')
        scraping = Artifact(proxy, 'services/process_mobile_scraping/data-available')
        top_apps = Intersect(app_ranks, scraping)

        apps_window = Artifact(proxy, 'services/current-mobile-apps-dates/window', required_value='true')

        google_scrape = Artifact(proxy, 'services/google_keywords/data-available')
        google_keywords = Intersect(google_scrape)

        return {
            'Web Analysis': web_analysis,
            'Apps': apps_window,
            'Top Apps': top_apps,
            'Google Scraping': google_keywords
        }

    @staticmethod
    def min_viable_options():
        return MINIMAL_VIABLE_DATES_SIZE

    @staticmethod
    def min_healthy_options():
        return DESRIED_DATES_SIZE

    @staticmethod
    def lookback_options():
        return LOOKBACK_SIZE