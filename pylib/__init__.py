import logging
from logging import config


#################################
#       pylib_log               #
#################################

# a bit ugly, but for now keeps us from overriding airflow's logger
# TODO once root cause of mrjob malfunction is resolved, restore logging
"""
if len(logging.root.handlers) == 0:
    import os
    curr_path = os.path.dirname(os.path.realpath(__file__)) + '/'
    config.fileConfig(curr_path + 'logging.cfg', disable_existing_loggers=False)


class ContextFilter(logging.Filter):
    CURRENT_LOG_NUM = 1

    def filter(self, record):
        record.count = self.CURRENT_LOG_NUM
        self.CURRENT_LOG_NUM += 1
        return True

logging.getLogger('hive').addFilter(ContextFilter())
"""