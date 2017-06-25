import logging
from logging import config


#################################
#       pylib_log               #
#################################

import os
curr_path = os.path.dirname(os.path.realpath(__file__)) + '/'
config.fileConfig(curr_path + 'logging.cfg')


class ContextFilter(logging.Filter):
    CURRENT_LOG_NUM = 1

    def filter(self, record):
        record.count = self.CURRENT_LOG_NUM
        self.CURRENT_LOG_NUM += 1
        return True

logging.getLogger('hive').addFilter(ContextFilter())
