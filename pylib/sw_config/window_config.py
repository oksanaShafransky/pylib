from airflow.models import Variable

from pylib.sw_config.data import Artifact, Intersect
from pylib.sw_config.kv_factory import provider_from_config

AIRFLOW_VAR_NAME_PREFIX = 'key_value_'

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
    def get_artifacts(purpose, env):
        proxy = provider_from_config(Variable.get('%s%s' % (AIRFLOW_VAR_NAME_PREFIX, env)))

        desktop_window = Artifact(proxy, '/'.join([purpose, env, 'services/current-web-dates/window']), required_value='true')
        mw_window = Artifact(proxy, '/'.join([purpose, env, 'services/current-mobile-web-dates/window']), required_value='true')
        web_analysis = Intersect(desktop_window, mw_window)

        app_ranks = Artifact(proxy, '/'.join([purpose, env, 'services/mobile-usage-ranks/data-available/window']))
        scraping = Artifact(proxy, '/'.join([purpose, env, 'services/process_mobile_scraping/data-available']))
        top_apps = Intersect(app_ranks, scraping)

        apps_window = Artifact(proxy, '/'.join([purpose, env, 'services/current-mobile-apps-dates/window']))

        google_scrape = Artifact(proxy, '/'.join([purpose, env, 'services/google_keywords/data-available']))
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
