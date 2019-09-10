from pylib.sw_config.data import Artifact, Intersect
import six

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
        apps = Intersect(apps_window, scraping)

        google_scrape = Artifact(proxy, 'services/google_keywords/data-available')
        google_keywords = Intersect(google_scrape)

        return {
            'Web Analysis': web_analysis,
            'Apps': apps,
            'Top Apps': top_apps,
            'Google Scraping': google_keywords
        }

    @staticmethod
    def get_artifacts_filtered(proxy, deletes):
        artifacts = SimilarWebWindowConfig.get_artifacts(proxy)
        filtered_artifacts = {}
        for name, artifact in six.iteritems(artifacts):
            if artifact.deletes_in_roots(deletes):
                filtered_artifacts[name] = artifact
        return filtered_artifacts

    @staticmethod
    def min_viable_options():
        return MINIMAL_VIABLE_DATES_SIZE

    @staticmethod
    def min_healthy_options():
        return DESRIED_DATES_SIZE

    @staticmethod
    def lookback_options():
        return LOOKBACK_SIZE
