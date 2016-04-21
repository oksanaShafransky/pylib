__author__ = 'Felix'

from data import *

MINIMAL_VIABLE_DATES_SIZE = 2
DESRIED_DATES_SIZE = 3


class SimilarWebWindowConfig:

    def __init__(self):
        pass

    # for reflection based tools
    def iter_fields(self):
        return []

    @staticmethod
    def get_artifacts():
        desktop_window = Artifact('services/current-web-dates/window', required_value='true')
        mw_window = Artifact('services/current-mobile-web-dates/window', required_value='true')
        web_analysis = Intersect(desktop_window, mw_window)

        app_engagement = Artifact('services/mobile-usage-ranks/data-available/window')
        scraping = Artifact('services/process_mobile_scraping/data-available')
        top_apps = Intersect(app_engagement, scraping)

        return {'Web Analysis': web_analysis, 'Top Apps': top_apps}


    @staticmethod
    def min_viable_options():
        return 8

    @staticmethod
    def min_healthy_options():
        return 8


